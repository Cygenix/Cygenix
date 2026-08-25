// netlify/functions/lib/rbac.js
//
// Role-based access control for the Cygenix Migration Console.
// Implements the role model from "User Roles, Permissions and Access
// Control v1.0" (17-Aug-2026): the Section 5 roles, the Section 6
// permission matrix AS DATA, the single can() policy function of
// Section 11.1, the separation-of-duties rules of Section 5.2, and the
// destructive-statement detection of Section 7.2 / R-17.
//
// Design rules carried over from the document:
//   - Deny by default. An action absent from the matrix is denied to
//     every role, including administrators (principle 1).
//   - The server decides. This module runs inside the Functions; the
//     browser mirror (cygenix-rbac.js) only hides controls as a courtesy.
//   - Control plane and data plane are separate: OW and PA hold no grant
//     that reads customer data or executes SQL (SoD-3). The one carve-out
//     is cancelling a run in flight, which PA holds as incident response.
//   - Environment, not intent, decides severity. PROD conditions are
//     evaluated here, from the connection's classification, defaulting
//     to PROD when unclassified (Section 7.1: the safe default is the
//     restrictive one).
//
// Phase note (Section 13): this is the Phase 0/1 build. Change requests,
// approvals and their SoD-1/SoD-2 enforcement points are Phase 2; the
// hooks exist (sodViolations) but nothing supplies an approval object
// yet. Until then a Production execute is permitted only to a Migration
// Lead and always audited at high severity — the Engineer never reaches
// Production (acceptance A-02), which is the half of the guardrail that
// can land before the approval workflow exists.
//
// No dependencies — the test suite loads this file directly, and the
// Azure Function can adopt it without pulling the Netlify tree with it.

'use strict';

const crypto = require('crypto');

// ── Roles (Section 5.1 / Appendix 16) ─────────────────────────────────────

const ROLES = {
  OW: { code: 'OW', name: 'Organisation Owner',      entra: 'Cygenix.OrgOwner',      scope: 'organisation', summary: 'Owns the tenant and the commercials. Touches no data.' },
  PA: { code: 'PA', name: 'Platform Administrator',  entra: 'Cygenix.PlatformAdmin', scope: 'organisation', summary: 'Grants access and holds secrets. Cannot use either.' },
  ML: { code: 'ML', name: 'Migration Lead',          entra: 'Cygenix.MigrationLead', scope: 'project',      summary: 'Runs the engagement and executes approved work.' },
  EN: { code: 'EN', name: 'Migration Engineer',      entra: 'Cygenix.Engineer',      scope: 'project',      summary: 'Builds the migration. Never reaches Production.' },
  AP: { code: 'AP', name: 'Approver',                entra: 'Cygenix.Approver',      scope: 'project',      summary: 'Authorises Production changes. Builds nothing.' },
  DO: { code: 'DO', name: 'Data Owner',              entra: 'Cygenix.DataOwner',     scope: 'project',      summary: 'Owns the data and the AI boundary. Signs off reconciliation.' },
  VA: { code: 'VA', name: 'Validator',               entra: 'Cygenix.Validator',     scope: 'project',      summary: 'Verifies independently. Writes nothing.' },
  AU: { code: 'AU', name: 'Auditor',                 entra: 'Cygenix.Auditor',       scope: 'organisation', summary: 'Sees everything, changes nothing, holds no secrets.' },
  MB: { code: 'MB', name: 'Member (baseline)',       entra: 'Cygenix.Member',        scope: 'organisation', summary: 'Signed in, read-only, nothing else. The default.' },
  SP: { code: 'SP', name: 'Automation Principal',    entra: 'Cygenix.Automation',    scope: 'project',      summary: 'Non-human. Runs pre-approved plans on a schedule.' },
};

const ROLE_CODES = Object.keys(ROLES);

// The first signer in an organisation bootstraps it (Section 12, step 1:
// "assign the first signer Organisation Owner and Platform Administrator
// so the tenant is not locked out"). ML and EN are added because today's
// customers are single-operator: without a data-plane role the first
// signer's working product would stop working the moment roles turned on.
// The Section 3 small-team note sanctions this — multiple roles per actor
// are permitted, SoD evaluates per act, and the collapsed segregation is
// surfaced (collapsedSegregation below) rather than hidden.
const BOOTSTRAP_ROLES = ['OW', 'PA', 'ML', 'EN'];

// ── Environments (Section 7.1) ────────────────────────────────────────────

const ENVIRONMENTS = ['DEV', 'TEST', 'STAGING', 'PROD'];
// "Newly discovered connections default to PROD — the safe default is the
// restrictive one, and an administrator downgrades deliberately."
const DEFAULT_ENVIRONMENT = 'PROD';

// Identity of a connection for classification purposes: where it points,
// not what credentials it carries. Server + database, normalised.
function connKey(server, database) {
  return String(server || '').trim().toLowerCase() + '|' + String(database || '').trim().toLowerCase();
}

// ── The permission matrix (Section 6), as data ────────────────────────────
//
// Grants: F full · L limited (evaluateLimit below defines the boundary)
//         A approve-only · R read-only. Absent = denied (principle 1).
//
// Only actions the product exposes TODAY are gated here; matrix rows whose
// object does not yet exist (change requests, reconciliation sign-off) are
// deliberately absent so deny-by-default covers them until their phase
// lands. Secret reveal, plaintext credential export and audit edit appear
// nowhere — those rows are denied to every role by design (SoD-4, SoD-5,
// R-25) and MUST NOT be added.

const MATRIX = {
  // Connect (6.1)
  'connection.view':       { OW: 'R', PA: 'F', ML: 'F', EN: 'F', AP: 'R', DO: 'R', VA: 'R', AU: 'R', MB: 'R' },
  'connection.edit':       { PA: 'F', ML: 'F', EN: 'L' },              // non-PROD; PROD collapses to PA via evaluate below
  'connection.classify':   { PA: 'F' },                                 // R-15: immutable to authoring roles
  'connection.test':       { PA: 'F', ML: 'F', EN: 'F', VA: 'F' },
  // Map & Build (6.2) — schema metadata is "view maps, schema and SQL"
  'schema.read':           { OW: 'R', ML: 'F', EN: 'F', AP: 'R', DO: 'R', VA: 'R', AU: 'R' },
  'sql.read':              { ML: 'F', EN: 'F', VA: 'F' },               // PROD → L (logged) via environment rule
  // Run (6.3). sql.write is the write path db-connect exposes today; the
  // matrix's "arbitrary write denied universally" holds once every write
  // routes through an approved run plan (Phase 2). Until then writes are
  // treated as run execution and inherit the environment conditions.
  'sql.write':             { ML: 'F', EN: 'F' },
  'run.execute':           { ML: 'F', EN: 'F' },                        // environment rule bars EN from STAGING(restricted)/PROD
  // Restore Database module: inspecting a backup (HEADERONLY/FILELISTONLY/
  // VERIFYONLY) is read-shaped; executing a RESTORE or BACKUP overwrites
  // whole databases — Lead only, and PROD-conditioned like any write.
  'restore.inspect':       { ML: 'F', EN: 'F' },
  'restore.execute':       { ML: 'F' },
  // Authorising somebody else's act. The 'A' grant finally has a row: the
  // Phase 2 note below said the approval hooks existed with nothing to
  // supply them, and lib/tenancy.js is what supplies them. An 'A' grant
  // cannot PERFORM the action (can() refuses it), which is the whole point —
  // the Approver authorises and builds nothing.
  'run.approve':           { AP: 'A', PA: 'A', ML: 'A', AU: 'R' },
  'run.cancel':            { PA: 'F', ML: 'F', EN: 'L' },               // SoD-3 carve-out: PA cancel is incident response, audited notice
  'run.rollback':          { ML: 'F', EN: 'L' },
  'schedule.create':       { ML: 'F', EN: 'L' },                        // EN may author, never enable
  'schedule.enable':       { ML: 'F' },
  // Report & Govern (6.5)
  'governance.set-floor':  { PA: 'L', DO: 'F', AU: 'R' },
  'audit.read':            { OW: 'R', PA: 'R', ML: 'L', EN: 'L', AP: 'L', DO: 'L', VA: 'L', AU: 'F' },
  'audit.export':          { PA: 'F', AU: 'F' },
  // Administration (6.6)
  'admin.read':            { OW: 'F', PA: 'F', AU: 'R' },
  'user.manage':           { OW: 'F', PA: 'F', AU: 'R' },
  'role.assign':           { OW: 'F', PA: 'F', AU: 'R' },
  'role.assign-admin':     { OW: 'F' },                                 // appointing PA (or OW) is Owner-only
  'project.create':        { OW: 'F', PA: 'F', ML: 'F' },
  // The console's own working state. The matrix had a row for creating an
  // engagement and none for the project record the pages read and write all
  // day, so that record was ungated — every signed-in caller could write it.
  // Reading is the baseline a Member holds; writing is delivery work, which
  // is the Lead's and the Engineer's. A signer with no role assignment
  // reaches neither (A-01).
  'project.read':          { OW: 'R', PA: 'R', ML: 'F', EN: 'F', AP: 'R', DO: 'R', VA: 'R', AU: 'R', MB: 'R' },
  'project.write':         { ML: 'F', EN: 'F' },
  // Tenant policy: who may change the guardrails the server enforces. This
  // is a control-plane act — it grants nobody data access, and an Auditor
  // reads it because a control they cannot see is a control they cannot
  // report on.
  'tenant.guardrails':     { OW: 'F', PA: 'F', AU: 'R' },
};

const GRANT_RANK = { F: 4, A: 3, L: 2, R: 1 };

// Best grant a set of roles holds for an action, or null (deny by default).
function grantFor(roles, action) {
  const row = MATRIX[action];
  if (!row) return null;
  let best = null;
  for (const r of roles || []) {
    const g = row[r];
    if (g && (!best || GRANT_RANK[g] > GRANT_RANK[best])) best = g;
  }
  return best;
}

// Actions that touch customer data or run SQL — the data plane (SoD-3).
const DATA_PLANE_ACTIONS = ['schema.read', 'sql.read', 'sql.write', 'run.execute', 'run.rollback'];

// ── The policy function (Section 11.1) ────────────────────────────────────
//
// decision = can(actor, action, resource)
//   actor:    { roles: ['ML','EN',...], oid, isActive }
//   resource: { environment: 'DEV|TEST|STAGING|PROD', restrictedStaging,
//               own (bool: actor started this run), mutating (bool) }
// Returns { allow, grant, severity, reason, logged } and never throws.
// Every caller writes an audit event with the outcome — allowed or denied
// (Section 10: "denied attempts are recorded as well as permitted ones").

function can(actor, action, resource) {
  resource = resource || {};
  const roles = (actor && actor.isActive !== false && actor.roles) || [];
  const env = ENVIRONMENTS.includes(resource.environment) ? resource.environment : DEFAULT_ENVIRONMENT;
  const prodLike = env === 'PROD' || (env === 'STAGING' && resource.restrictedStaging !== false);

  const deny = (reason) => ({ allow: false, grant: null, severity: prodLike ? 'high' : 'notice', reason, environment: env });
  const allow = (grant, extra) => Object.assign(
    { allow: true, grant, severity: prodLike && resource.mutating ? 'high' : 'info', reason: 'granted', environment: env, logged: prodLike },
    extra || {});

  if (!roles.length) return deny('no active role assignment');           // A-01: unassigned reaches nothing
  const grant = grantFor(roles, action);
  if (!grant) return deny('no grant for ' + action + ' (deny by default)');
  if (grant === 'R') {
    // Read-only grant satisfies only non-mutating acts.
    return resource.mutating ? deny('read-only grant cannot mutate') : allow('R');
  }
  if (grant === 'A') return deny('approve-only grant cannot perform the action');

  // Environment conditioning (Sections 6.3, 7.2).
  if (prodLike) {
    if (action === 'run.execute' || action === 'sql.write') {
      // Engineer never reaches Production (A-02). Lead may, at high
      // severity, until the Phase 2 change-request gate replaces this.
      if (!roles.includes('ML')) return deny('Production execution requires the Migration Lead role');
      return allow(grant, { guardrail: 'production' });
    }
    if (action === 'connection.edit' && !roles.includes('PA')) {
      return deny('Production connections are administrator-managed');    // 6.1
    }
    if (action === 'sql.read' || action === 'schema.read') {
      // 6.2: permitted, logged, through the read path only.
      return allow(grant === 'F' ? 'L' : grant, { logged: true });
    }
  }

  if (grant === 'L') return evaluateLimit(roles, action, resource, env, deny, allow);
  return allow(grant);
}

// The L boundaries, one per action, from the notes beneath each matrix
// table in Section 6.
function evaluateLimit(roles, action, resource, env, deny, allow) {
  switch (action) {
    case 'connection.edit':   // EN: non-Production only, no secrets, no reclassify
      return env === 'PROD' ? deny('Engineers may not edit Production connections') : allow('L');
    case 'run.cancel':        // EN: own runs only, never on Production
    case 'run.rollback':
      if (roles.includes('ML') || roles.includes('PA')) return allow('F');
      if (env === 'PROD') return deny('Engineers may not ' + action.split('.')[1] + ' on Production');
      return resource.own ? allow('L') : deny('Engineers may only ' + action.split('.')[1] + ' runs they started');
    case 'schedule.create':   // EN may author a schedule but not enable one
      return resource.enable ? deny('Engineers may author a schedule but not enable one') : allow('L');
    case 'audit.read':        // delivery roles read their own trail, not the organisation's
      return allow('L', { selfOnly: true });
    case 'governance.set-floor': // PA sets the floor, never the operative mode
      return allow('L', { floorOnly: true });
    default:
      return deny('no limit evaluator for ' + action);                  // fail closed
  }
}

// ── Separation of duties (Section 5.2) ────────────────────────────────────

// SoD-7: pairs of roles that collapse a segregation when one actor holds
// both. The act is still refused per-rule; this names what to surface on
// the governance panel.
const CONFLICTING_PAIRS = [
  ['EN', 'AP'], ['ML', 'AP'],   // author/executor vs approver (SoD-1/2)
  ['PA', 'ML'], ['PA', 'EN'],   // control plane vs data plane (SoD-3)
  ['OW', 'ML'], ['OW', 'EN'],
];

function collapsedSegregation(roles) {
  const held = new Set(roles || []);
  return CONFLICTING_PAIRS.filter(([a, b]) => held.has(a) && held.has(b))
    .map(([a, b]) => ({ pair: a + '+' + b, note: ROLES[a].name + ' + ' + ROLES[b].name }));
}

// SoD-1/2 evaluators for the Phase 2 approval workflow. Exposed now so the
// change-request endpoints land against a tested rule, not a rewrite.
function sodViolations(act, ctx) {
  const v = [];
  if (act === 'approve' && ctx.authorChain && ctx.authorChain.includes(ctx.actorOid)) {
    v.push({ rule: 'SoD-1', reason: 'the author of a change cannot approve it' });
  }
  if (act === 'execute' && ctx.approvers && ctx.approvers.length === 1 && ctx.approvers[0] === ctx.actorOid) {
    v.push({ rule: 'SoD-2', reason: 'the sole approver of a change cannot execute it' });
  }
  return v;
}

// ── Role assignment validation (Section 6.6) ──────────────────────────────
//
// Pure rule check used by the admin endpoint. Throws nothing; returns
// { ok } or { ok:false, reason }.
//   - Assigning or revoking OW or PA requires OW ("Appoint a Platform
//     Administrator: OW only"). Everything else needs role.assign.
//   - The last active administrator cannot be removed or deactivated —
//     Section 12's lockout concern, made a rule.
function validateAssignmentChange({ actorRoles, op, targetRole, remainingAdmins }) {
  if (!ROLE_CODES.includes(targetRole)) return { ok: false, reason: 'unknown role ' + targetRole };
  if (targetRole === 'SP') return { ok: false, reason: 'the Automation Principal is not assignable to a person' };
  const adminChange = targetRole === 'OW' || targetRole === 'PA';
  const gate = adminChange ? 'role.assign-admin' : 'role.assign';
  const g = grantFor(actorRoles, gate);
  if (g !== 'F') return { ok: false, reason: adminChange ? 'appointing an administrator requires the Organisation Owner' : 'no grant to assign roles' };
  if (op === 'revoke' && adminChange && remainingAdmins !== undefined && remainingAdmins < 1) {
    return { ok: false, reason: 'refusing to remove the last administrator — the organisation would be locked out' };
  }
  return { ok: true };
}

// ── Entra app roles (Section 9) ───────────────────────────────────────────

// Map the validated token's roles claim (Cygenix.* app role values) onto
// role codes. Unknown values are ignored, never guessed. The claim comes
// from verifyAuthHeader's VERIFIED payload only (Section 9.5).
function rolesFromClaims(claims) {
  const values = (claims && Array.isArray(claims.roles)) ? claims.roles : [];
  const byEntra = {};
  for (const c of ROLE_CODES) byEntra[ROLES[c].entra] = c;
  return values.map(v => byEntra[v]).filter(Boolean);
}

// Effective roles: union of Cygenix-stored assignments and token app roles.
// Stored assignments give R-04 its teeth — revocation (or deactivation)
// takes effect on the next request, not the next sign-in, because an
// inactive user resolves to no roles regardless of what the token asserts.
function effectiveRoles(storedRoles, tokenRoles, user) {
  if (user && user.isActive === false) return [];
  return [...new Set([...(storedRoles || []), ...(tokenRoles || [])])].filter(r => ROLE_CODES.includes(r));
}

// ── Destructive-statement detection (R-17 / Section 7.2) ──────────────────
//
// "Statements that delete, truncate, drop or update without a bounded
// predicate are parsed out, listed individually." Lexical, not a full
// parser: strings and comments are stripped first so a literal containing
// the word DELETE is not a finding, and boundedness means the statement
// carries a WHERE. Conservative by design — a false positive costs the
// operator a look; a false negative costs them a table.

function stripSqlNoise(sql) {
  let s = String(sql || '');
  s = s.replace(/--[^\n]*/g, ' ');                 // line comments
  s = s.replace(/\/\*[\s\S]*?\*\//g, ' ');          // block comments
  s = s.replace(/'(?:[^']|'')*'/g, "''");           // string literals ('' escape)
  s = s.replace(/N''/g, "''");
  return s;
}

function detectDestructive(sql) {
  const findings = [];
  for (const stmtRaw of stripSqlNoise(sql).split(';')) {
    const stmt = stmtRaw.trim();
    if (!stmt) continue;
    const up = stmt.toUpperCase();
    let m;
    if ((m = /^DELETE\s+(?:FROM\s+)?([[\]\w."]+)/i.exec(stmt))) {
      findings.push({ type: 'DELETE', table: m[1], bounded: /\bWHERE\b/.test(up) });
    } else if ((m = /^TRUNCATE\s+TABLE\s+([[\]\w."]+)/i.exec(stmt))) {
      findings.push({ type: 'TRUNCATE', table: m[1], bounded: false });
    } else if ((m = /^DROP\s+(TABLE|VIEW|PROCEDURE|FUNCTION|INDEX|TRIGGER|DATABASE|SCHEMA)\s+(?:IF\s+EXISTS\s+)?([[\]\w."]+)/i.exec(stmt))) {
      findings.push({ type: 'DROP ' + m[1].toUpperCase(), table: m[2], bounded: false });
    } else if ((m = /^UPDATE\s+([[\]\w."]+)/i.exec(stmt))) {
      findings.push({ type: 'UPDATE', table: m[1], bounded: /\bWHERE\b/.test(up) });
    }
  }
  return findings;
}

// Read/write classification for the SQL paths db-connect exposes. Anything
// carrying a mutation keyword anywhere in the statement is a write — a CTE
// that feeds an INSERT, or SELECT ... INTO, must not slip through as a read.
const WRITE_KEYWORDS = /\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|DROP|ALTER|CREATE|EXEC|EXECUTE|GRANT|REVOKE|DENY|INTO)\b/i;

function isReadOnlySql(sql) {
  const s = stripSqlNoise(sql);
  if (!s.trim()) return true;
  for (const stmtRaw of s.split(';')) {
    const stmt = stmtRaw.trim();
    if (!stmt) continue;
    if (!/^(SELECT|WITH|SET\s+(?:NOCOUNT|ANSI|TRANSACTION\s+ISOLATION)|USE)\b/i.test(stmt)) return false;
    if (WRITE_KEYWORDS.test(stmt)) return false;
  }
  return true;
}

// ── Audit hash chain (Section 10) ─────────────────────────────────────────
//
// entry_hash = sha256(prev_hash + canonical entry). Any excision breaks
// the chain and is detectable by re-walking it (verifyEntries). Storage
// and the append path live in org-store.js; the maths lives here so it is
// testable without I/O.

function hashEntry(prevHash, entry) {
  const clone = { ...entry };
  delete clone.entryHash;
  return crypto.createHash('sha256')
    .update(String(prevHash || '') + JSON.stringify(clone, Object.keys(clone).sort()))
    .digest('hex');
}

function chainEntry(prevHash, entry) {
  const e = { ...entry, prevHash: prevHash || '' };
  e.entryHash = hashEntry(prevHash, e);
  return e;
}

function verifyEntries(entries) {
  let prev = '';
  for (let i = 0; i < entries.length; i++) {
    const e = entries[i];
    if ((e.prevHash || '') !== prev) return { ok: false, brokenAt: e.seq, reason: 'prev-hash mismatch' };
    if (hashEntry(e.prevHash, e) !== e.entryHash) return { ok: false, brokenAt: e.seq, reason: 'entry hash mismatch' };
    prev = e.entryHash;
  }
  return { ok: true, count: entries.length };
}

module.exports = {
  ROLES, ROLE_CODES, BOOTSTRAP_ROLES,
  ENVIRONMENTS, DEFAULT_ENVIRONMENT, connKey,
  MATRIX, DATA_PLANE_ACTIONS, grantFor,
  can, collapsedSegregation, sodViolations, CONFLICTING_PAIRS,
  validateAssignmentChange,
  rolesFromClaims, effectiveRoles,
  stripSqlNoise, detectDestructive, isReadOnlySql,
  hashEntry, chainEntry, verifyEntries,
};
