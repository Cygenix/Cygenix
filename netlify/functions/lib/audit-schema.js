// netlify/functions/lib/audit-schema.js
//
// The shape of an audit event, and the three decisions that have to be made
// about one before it is allowed anywhere near the hash chain: what category
// it belongs to, what must be scrubbed out of it, and whether it is a fact
// the server observed or a claim the browser made.
//
// Pure. No I/O, no blobs, no clock of its own unless one is handed in — so
// every rule below is testable from a fixture rather than from a deployment.
// The storage and chaining live in lib/org-store.js; the endpoints live in
// netlify/functions/audit.js.
//
// ── Why this is an EXTENSION and not a second store ───────────────────────
//
// There is already an append-only, hash-chained trail (rbac.chainEntry /
// verifyEntries over audit/e/<seq> in the cygenix-org blob store). It holds
// RBAC-gated acts — sql.write, role.assign, tenant.create — and the
// governance story the product sells rests on the fact that it is ONE chain
// that can be walked end to end. A second store for "product events" would
// produce two trails that disagree, and an auditor asked which one is the
// record would be right to distrust both.
//
// So every field the existing entry carries is carried forward unchanged:
//
//   seq occurredAt actorOid actorEmail effectiveRoles action resourceType
//   resourceId environment outcome severity detail prevHash entryHash
//
// and the richer schema is ADDED alongside it (id, category, actorType,
// actorName, onBehalfOf, projectId, target, changes, summary, context,
// source). Nothing is renamed. `occurredAt` is not renamed to `ts` and
// `entryHash` is not renamed to `hash`, because entries written before this
// module existed are still in the chain and still have to verify, and
// because readAudit() and the Users & Roles screen read those names today.
//
// ── Why `source` exists ───────────────────────────────────────────────────
//
// POST /record is open to any signed-in caller, because a great many of the
// things worth recording — an export, a reorder, a bulk generate — happen
// entirely in the browser and never reach a function. That is a real hole:
// a signed-in caller can put rows into the evidence chain. Three things
// close it to the width of an honest record rather than a lie:
//
//   1. The actor, the timestamp and the hash are filled in HERE, from the
//      verified token. Anything the body says about who did it is dropped.
//   2. CLIENT_ACTIONS is an allowlist. An action not on it is refused, so
//      the browser cannot assert `role.assign` or `sql.write`.
//   3. Every such entry is stamped source:'client'. An auditor reading the
//      trail can tell a fact the server observed from a claim the browser
//      made, which is the distinction that actually matters and the one a
//      single undifferentiated stream destroys.
//
// Server-side call sites pass source:'server' and are not allowlisted —
// they are already behind authz.authorize().

'use strict';

const crypto = require('crypto');
const rbac = require('./rbac');

// ── Categories (brief Section 5) ──────────────────────────────────────────
//
// `alwaysOn` is not a default and not a UI hint: it is the set the capture
// state cannot switch off. Pausing the log because a bulk import is noisy
// must never also stop recording a sign-in, a role change or a Production
// write — that is precisely the window someone would choose. The settings
// endpoint rejects an attempt to disable one of these, and so does the UI;
// the endpoint is the one that counts.

const CATEGORIES = [
  { key: 'security',    label: 'Security',           alwaysOn: true,
    detail: 'Sign-in, failed sign-in, MFA, API keys.' },
  { key: 'access',      label: 'Access & roles',     alwaysOn: true,
    detail: 'Users, invitations, role changes, organisation bootstrap.' },
  { key: 'prod',        label: 'Production changes', alwaysOn: true,
    detail: 'Any write or configuration change against a PROD connection.' },
  { key: 'audit',       label: 'The log itself',     alwaysOn: true,
    detail: 'State changes, settings changes, exports, verification, retention purges.' },
  { key: 'settings',    label: 'Settings',           alwaysOn: false,
    detail: 'Project settings, notifications, system parameters.' },
  { key: 'connections', label: 'Connections',        alwaysOn: false,
    detail: 'Connection create, edit, test, delete and saved secrets.' },
  { key: 'mapping',     label: 'Mapping & SQL',      alwaysOn: false,
    detail: 'Object mapping, schema explorer, SQL editor saves, AI Assist applies.' },
  { key: 'jobs',        label: 'Jobs',               alwaysOn: false,
    detail: 'Create, run, cancel, reorder, delete, restore, packages, scheduler.' },
  { key: 'stream',      label: 'Data Stream',        alwaysOn: false,
    detail: 'Stream create, pause, resume and delete.' },
  { key: 'data',        label: 'Data out',           alwaysOn: false,
    detail: 'CSV exports, client packs, script and package exports.' },
  { key: 'projects',    label: 'Projects',           alwaysOn: false,
    detail: 'Project create, archive and scope change.' },
];

const CATEGORY_KEYS = CATEGORIES.map(c => c.key);
const ALWAYS_ON = CATEGORIES.filter(c => c.alwaysOn).map(c => c.key);

function isAlwaysOn(category) { return ALWAYS_ON.indexOf(category) !== -1; }

// Where an action lands when the caller does not say. Prefix match on the
// resource half of `<resource>.<verb>`, longest first, then a default of
// 'settings' — never a silent drop, because an uncategorised event that
// vanishes is worse than one filed in the wrong drawer.
const CATEGORY_BY_PREFIX = {
  'audit':      'audit',
  'auth':       'security',
  'session':    'security',
  'apikey':     'security',
  'mfa':        'security',
  'role':       'access',
  'user':       'access',
  'tenant':     'access',
  'org':        'access',
  'member':     'access',
  'invite':     'access',
  'sql':        'mapping',
  'schema':     'mapping',
  'mapping':    'mapping',
  'objectmap':  'mapping',
  'connection': 'connections',
  'conn':       'connections',
  'job':        'jobs',
  'jobs':       'jobs',
  'run':        'jobs',
  'schedule':   'jobs',
  'package':    'jobs',
  'restore':    'jobs',
  'stream':     'stream',
  'datastream': 'stream',
  'data':       'data',
  'export':     'data',
  'report':     'data',
  'project':    'projects',
  'projects':   'projects',
  'sysparam':   'settings',
  'settings':   'settings',
  'notify':     'settings',
  'governance': 'settings',
};

function categoryFor(action, environment) {
  // Environment wins over resource. A configuration change against PROD is
  // a Production change first and a settings change second — that is the
  // whole point of `prod` being always-on, and filing it under `settings`
  // would let a pause swallow it.
  if (environment === 'PROD') return 'prod';
  const head = String(action || '').split('.')[0].toLowerCase();
  return CATEGORY_BY_PREFIX[head] || 'settings';
}

// ── What the browser is allowed to assert ─────────────────────────────────
//
// Everything on this list is something that genuinely happens in the page
// and nowhere else. Nothing on it grants anything, changes a role, touches
// a Production connection or moves money. Adding a row here is a security
// decision and shows up in the diff as one.
const CLIENT_ACTIONS = {
  'data.export-scripts':  'data',
  'data.export-package':  'data',
  'data.export-csv':      'data',
  'data.client-pack':     'data',
  'jobs.complete':        'jobs',
  'jobs.reorder':         'jobs',
  'jobs.bulk-generate':   'jobs',
  'jobs.validate':        'jobs',
  'jobs.setup-check':     'jobs',
  'jobs.rename':          'jobs',
  'jobs.trash':           'jobs',
  'jobs.restore':         'jobs',
  'jobs.delete':          'jobs',
  'schedule.create':      'jobs',
  'schedule.update':      'jobs',
  'schedule.enable':      'jobs',
  'schedule.delete':      'jobs',
  // A run the operator started from the Task Agent. The scheduler function
  // owns the run's OUTCOME; this records only that a person asked for it,
  // which is the half that happens in the browser and nowhere else.
  'run.execute':          'jobs',
  'mapping.ai-apply':     'mapping',
  'mapping.save':         'mapping',
  'sql.save':             'mapping',
  'sql.delete':           'mapping',
  'connection.test':      'connections',
  'connection.create':    'connections',
  'connection.edit':      'connections',
  'connection.delete':    'connections',
  // The API key lives only in the browser — there is no server that ever
  // sees it, so there is no server-side place to observe it being set or
  // cleared from. Letting the browser assert these two is a deliberate
  // widening into an always-on category, and it is bounded the same way
  // everything else here is: the actor comes from the token, the value is
  // never sent, and the entry is stamped source:'client' so a reader can
  // see it is asserted rather than observed.
  'apikey.set':           'security',
  'apikey.revoke':        'security',
  'settings.update':      'settings',
  'sysparam.update':      'settings',
  // The Data Stream engine's own action names — they are what
  // cygenix-datastream.js already writes into its internal list, and using
  // the same strings means one vocabulary rather than a translation layer
  // that can drift.
  'stream.created':          'stream',
  'stream.updated':          'stream',
  'stream.started':          'stream',
  'stream.paused':           'stream',
  'stream.resumed':          'stream',
  'stream.stopped':          'stream',
  'stream.deleted':          'stream',
  'stream.retention':        'stream',
  'stream.checkpoint_reset': 'stream',
  'stream.cutover_begin':    'stream',
  'project.create':       'projects',
  'project.update':       'projects',
  'project.archive':      'projects',
  'project.delete':       'projects',
  'data.export-pdf':      'data',
};

function isClientAction(action) {
  return Object.prototype.hasOwnProperty.call(CLIENT_ACTIONS, String(action || ''));
}

// ── Redaction (brief Section 7) ───────────────────────────────────────────
//
// This runs BEFORE the entry is hashed, so a secret never enters the chain
// in the first place. That ordering is not an optimisation — the chain is
// append-only and tamper-evident by design, which means a secret written
// into it cannot afterwards be removed without breaking verification for
// every entry that follows. There is no second chance here.
//
// The name test is deliberately broad and deliberately dumb. A field called
// `apiKeyLabel` is redacted even though a label is harmless, because the
// cost of that is an auditor seeing "(redacted)" where they wanted a word,
// and the cost of the opposite is a live credential in an evidence pack
// that gets emailed to a client.

const SECRET_FIELD_RE = /pass(word)?|secret|token|key|conn(ection)?str|credential/i;
const REDACTED = '(redacted)';

// Connection strings hide a password inside a value whose FIELD name may be
// perfectly innocent ("target", "dsn", "source"). Both the ADO/ODBC form
// (Password=… / pwd=…) and the URI form (postgres://user:pw@host) are
// handled, because this product speaks to SQL Server, Azure SQL and
// PostgreSQL and gets given both.
function redactConnectionString(value) {
  let s = String(value);
  s = s.replace(/\b(pass(?:word)?|pwd)\s*=\s*(?:"[^"]*"|'[^']*'|\{[^}]*\}|[^;]*)/gi,
                (m, k) => k + '=' + REDACTED);
  s = s.replace(/\b(AccountKey|SharedAccessSignature|sig)\s*=\s*[^;]*/gi,
                (m, k) => k + '=' + REDACTED);
  s = s.replace(/([a-z][a-z0-9+.-]*:\/\/[^:/?#@\s]+):[^@/\s]*@/gi, '$1:' + REDACTED + '@');
  return s;
}

function looksLikeConnectionString(value) {
  if (typeof value !== 'string' || value.length < 8) return false;
  return /\b(pass(?:word)?|pwd|accountkey|sharedaccesssignature)\s*=/i.test(value)
      || /^[a-z][a-z0-9+.-]*:\/\/[^:/?#@\s]+:[^@/\s]+@/i.test(value);
}

// Deep, structure-preserving. Arrays keep their length and objects keep
// their keys, because an auditor needs to see that a field CHANGED even
// when they may not see what it changed to — "(redacted) → (redacted)" on
// a password field is a real and useful fact.
function redact(value, keyName, seen) {
  seen = seen || new Set();
  if (value === null || value === undefined) return value;

  if (typeof value === 'string') {
    if (keyName && SECRET_FIELD_RE.test(keyName)) return REDACTED;
    return looksLikeConnectionString(value) ? redactConnectionString(value) : value;
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return (keyName && SECRET_FIELD_RE.test(keyName)) ? REDACTED : value;
  }
  if (typeof value !== 'object') return undefined;   // functions, symbols: gone

  // A cycle in a detail object would otherwise hang the request thread.
  if (seen.has(value)) return '(circular)';
  seen.add(value);

  if (Array.isArray(value)) {
    // The array's own key name governs its members: `passwords: [a, b]`.
    return value.map(v => redact(v, keyName, seen));
  }
  const out = {};
  for (const k of Object.keys(value)) {
    const r = redact(value[k], k, seen);
    if (r !== undefined) out[k] = r;
  }
  return out;
}

// A pre-built changes array loses the one piece of context redact() relies
// on: the field's NAME lives in `row.field`, not in the key holding the
// value, so a plain deep redact would test the literal keys "before" and
// "after" and let a password through. Every changes array is therefore run
// through here, whether diff() built it or a caller did.
function redactChanges(changes) {
  if (!Array.isArray(changes)) return null;
  return changes
    .filter(r => r && typeof r === 'object')
    .map(r => ({
      field:  String(r.field == null ? '' : r.field),
      before: redact(r.before === undefined ? null : r.before, r.field),
      after:  redact(r.after  === undefined ? null : r.after,  r.field),
    }));
}

// ── Before/after diffs ────────────────────────────────────────────────────
//
// Only changed fields, so a settings save of forty fields where one moved
// produces one row rather than forty. Comparison is by JSON value, which
// makes 5000 and '5000' different — that is intentional: a type change in a
// stored setting is exactly the sort of thing worth seeing in a diff.
function diff(before, after, fields) {
  const b = before || {}, a = after || {};
  const keys = Array.isArray(fields) && fields.length
    ? fields.slice()
    : [...new Set([...Object.keys(b), ...Object.keys(a)])];
  const out = [];
  for (const f of keys) {
    const bv = b[f], av = a[f];
    if (JSON.stringify(bv === undefined ? null : bv) === JSON.stringify(av === undefined ? null : av)) continue;
    out.push({
      field: f,
      before: redact(bv === undefined ? null : bv, f),
      after:  redact(av === undefined ? null : av, f),
    });
  }
  return out;
}

// ── Identifiers ───────────────────────────────────────────────────────────
//
// ULID rather than UUID v4 so ids sort by time, which makes a page of them
// readable and makes a tie-break on equal timestamps deterministic. `seq`
// remains the chain's identity; this is the stable public id an export or a
// support ticket can quote.
const B32 = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

function ulid(now, randomBytes) {
  let t = Math.floor(now === undefined ? Date.now() : now);
  let time = '';
  for (let i = 0; i < 10; i++) { time = B32[t % 32] + time; t = Math.floor(t / 32); }
  const bytes = randomBytes || crypto.randomBytes(10);
  let rand = '';
  for (let i = 0; i < 16; i++) rand += B32[bytes[i % bytes.length] % 32];
  return time + rand;
}

// ── Building the storable entry ───────────────────────────────────────────
//
// One function, used by every caller, so there is exactly one place where
// "the actor comes from the token" is true. `input` is what the caller
// asked to record; `ctx` is what the server knows. Where they disagree, ctx
// wins — silently, not with an error, because a body that carries an actor
// is far more likely to be a hopeful client library than an attack, and
// refusing it would lose the event as well as the lie.

const OUTCOMES = ['allowed', 'denied', 'failed'];

function buildEntry(input, ctx) {
  input = input || {};
  ctx = ctx || {};
  const actor = ctx.actor || {};
  const at = ctx.now ? new Date(ctx.now) : new Date();

  const environment = rbac.ENVIRONMENTS.indexOf(input.environment || input.env) !== -1
    ? (input.environment || input.env)
    : null;

  const category = CATEGORY_KEYS.indexOf(input.category) !== -1
    ? (environment === 'PROD' ? 'prod' : input.category)
    : categoryFor(input.action, environment);

  const outcome = OUTCOMES.indexOf(input.outcome) !== -1 ? input.outcome : 'allowed';

  // A target is the human-facing "what was changed". resourceType and
  // resourceId are the columns the existing trail and the Users & Roles
  // screen already read, so a target is projected onto them rather than
  // replacing them — old readers keep working, new ones get the label.
  const target = input.target && typeof input.target === 'object'
    ? {
        type:  input.target.type || input.resourceType || null,
        id:    input.target.id != null ? String(input.target.id) : (input.resourceId || null),
        label: input.target.label || null,
      }
    : null;

  const actorType = ['user', 'system', 'assistant'].indexOf(input.actorType) !== -1
    ? input.actorType : 'user';

  const entry = {
    // ── existing chain fields, names unchanged ──
    occurredAt: at.toISOString(),
    actorOid:   actor.oid || (actorType === 'system' ? 'system' : null),
    actorEmail: actor.email || (actorType === 'system' ? 'system' : null),
    effectiveRoles: Array.isArray(actor.roles) ? actor.roles : [],
    action:     String(input.action || 'unknown'),
    resourceType: (target && target.type) || input.resourceType || null,
    resourceId:   (target && target.id)   || input.resourceId   || null,
    environment,
    outcome,
    severity:   input.severity || (outcome !== 'allowed' ? 'notice'
                 : (environment === 'PROD' ? 'high' : 'info')),
    detail:     input.detail ? redact(input.detail) : null,

    // ── added by this module ──
    id:         input.id || ulid(at.getTime(), ctx.randomBytes),
    category,
    actorType,
    actorName:  actor.name || null,
    // Ask Cygenix acts as a person, not as itself. onBehalfOf is the person
    // who asked; actorEmail stays the signed-in identity the token carried,
    // so the two can never silently become the same field.
    onBehalfOf: actorType === 'assistant' ? (actor.email || input.onBehalfOf || null) : null,
    tenantId:   ctx.tenantId || null,
    projectId:  input.projectId != null ? String(input.projectId) : null,
    target,
    changes:    redactChanges(input.changes),
    summary:    input.summary ? String(input.summary).slice(0, 500) : null,
    context:    redact({
      ip:        ctx.ip || null,
      userAgent: ctx.userAgent ? String(ctx.userAgent).slice(0, 300) : null,
      requestId: ctx.requestId || null,
      sessionId: input.sessionId || null,
      route:     ctx.route || null,
    }),
    // 'server' = the function observed it. 'client' = the browser said so.
    source:     ctx.source === 'client' ? 'client' : 'server',
  };
  return entry;
}

// ── The compact index row ─────────────────────────────────────────────────
//
// readAudit() costs one blob GET per entry. Filtering a year of events by
// actor and category that way is thousands of round-trips inside a 26-second
// function, so each append also writes a projection into a per-month index
// and every list, filter and KPI query reads THOSE. Keep this small: it is
// read whole, and every field added to it is paid for on each query.
function indexRow(entry, seq) {
  return {
    seq: seq != null ? seq : entry.seq,
    id: entry.id,
    ts: entry.occurredAt,
    a: entry.actorEmail || null,
    at: entry.actorType,
    ac: entry.action,
    c: entry.category,
    o: entry.outcome,
    e: entry.environment,
    p: entry.projectId,
    s: entry.source,
    // The searchable text for `q`, lower-cased once here rather than on
    // every row of every query.
    t: [entry.action, entry.actorEmail, entry.summary,
        entry.target && entry.target.label, entry.target && entry.target.id]
       .filter(Boolean).join(' ').toLowerCase().slice(0, 300),
    // Just the target, so the TARGET column's own filter narrows on the
    // target and not on an action or an actor that happens to share a word.
    // Rows written before this field existed do not have it; queryAudit
    // falls back to `t` for those rather than silently matching nothing,
    // which would make old history look like it had no targets at all.
    tg: [entry.target && entry.target.label, entry.target && entry.target.id,
         entry.resourceType]
       .filter(Boolean).join(' ').toLowerCase().slice(0, 200),
  };
}

function indexKeyFor(iso) {
  return 'audit/idx/' + String(iso || '').slice(0, 7);   // audit/idx/2026-09
}

// ── The retention checkpoint ──────────────────────────────────────────────
//
// Retention purging and a hash chain are in direct conflict: deleting entry
// 1..N breaks the walk for every entry after them, because verification
// starts from '' and each prevHash refers to a row that is gone.
//
// The resolution is to record, at the purge boundary, what the chain looked
// like there — the last surviving sequence and its hash — and start
// verification from that anchor instead of from ''. The checkpoint is
// itself written into the chain as an `audit.retention.purge` entry, so the
// act of purging is evidence rather than a hole.
//
// This format ships NOW, before the purge job exists, so that entries
// written today can be verified against a checkpoint written later without
// re-chaining anything. It is cheap to define early and expensive to
// retrofit.
function checkpoint(entry, purgedCount, retentionDays) {
  return {
    version: 1,
    anchorSeq: entry.seq,
    anchorHash: entry.entryHash,
    anchorAt: entry.occurredAt,
    purgedThroughSeq: entry.seq - 1,
    purgedCount: purgedCount || 0,
    retentionDays: retentionDays || null,
    createdAt: new Date().toISOString(),
  };
}

module.exports = {
  CATEGORIES, CATEGORY_KEYS, ALWAYS_ON, isAlwaysOn, categoryFor,
  CLIENT_ACTIONS, isClientAction,
  SECRET_FIELD_RE, REDACTED, redact, redactChanges, redactConnectionString,
  looksLikeConnectionString,
  diff, ulid, buildEntry, OUTCOMES,
  indexRow, indexKeyFor, checkpoint,
};
