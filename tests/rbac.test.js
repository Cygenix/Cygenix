// tests/rbac.test.js — the role model, permission matrix and policy
// function from the Roles, Permissions & Access Control spec (v1.0).
//
// The spec's own words (Section 14): "Negative tests matter more than
// positive ones here: proving an Engineer can run a job is easy, and
// proving they cannot is the point." Each block cites the acceptance
// criterion or SoD rule it evidences.

'use strict';

const R = require('../netlify/functions/lib/rbac');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('RBAC — role model and policy function\n');

// ── The role set ──────────────────────────────────────────────────────────
check('all ten roles from Section 5.1 exist',
  ['OW','PA','ML','EN','AP','DO','VA','AU','MB','SP'].every(c => R.ROLES[c]));
check('every role carries its Entra app role value (Section 9.2)',
  R.ROLE_CODES.every(c => /^Cygenix\./.test(R.ROLES[c].entra)));
check('the baseline Member role exists and is organisation-scoped',
  R.ROLES.MB.scope === 'organisation');
check('bootstrap grants owner+admin plus the working data-plane pair',
  JSON.stringify(R.BOOTSTRAP_ROLES) === JSON.stringify(['OW','PA','ML','EN']));

// ── Deny by default (principle 1) ─────────────────────────────────────────
const every = (roles, action, res) => R.can({ roles }, action, res);
check('an action absent from the matrix is denied to every role',
  R.ROLE_CODES.every(c => !every([c], 'made.up-action', {}).allow));
check('an actor with no roles is denied everything (A-01)',
  !every([], 'connection.view', {}).allow && !every([], 'sql.read', {}).allow);
check('an inactive actor is denied regardless of roles (R-04)',
  !R.can({ roles: ['ML'], isActive: false }, 'sql.read', { environment: 'DEV' }).allow);
check('the baseline Member can view but never mutate (A-01)',
  every(['MB'], 'connection.view', {}).allow
  && !every(['MB'], 'connection.view', { mutating: true }).allow
  && !every(['MB'], 'sql.read', { environment: 'DEV' }).allow
  && !every(['MB'], 'run.execute', { environment: 'DEV' }).allow);

// ── Rows that must not exist (SoD-4, SoD-5, R-25) ────────────────────────
check('no action exists to reveal a secret, export credentials or edit audit',
  !R.MATRIX['secret.reveal'] && !R.MATRIX['credentials.export'] && !R.MATRIX['audit.edit']
  && !R.MATRIX['audit.delete']);
check('and being unlisted, they are denied even to the Owner',
  !every(['OW','PA','AU'], 'secret.reveal', {}).allow
  && !every(['OW','PA','AU'], 'audit.edit', {}).allow);

// ── Control plane vs data plane (SoD-3, A-05) ────────────────────────────
check('the Platform Administrator holds no data-plane grant',
  R.DATA_PLANE_ACTIONS.every(a => !every(['PA'], a, { environment: 'DEV' }).allow));
check('nor does the Organisation Owner (beyond read-only views)',
  !every(['OW'], 'sql.read', { environment: 'DEV' }).allow
  && !every(['OW'], 'run.execute', { environment: 'DEV' }).allow
  && !every(['OW'], 'sql.write', { environment: 'DEV' }).allow);
check('the PA cancel carve-out is the one exception, and it is explicit',
  every(['PA'], 'run.cancel', { environment: 'PROD' }).allow);

// ── Production guardrails (Section 7.2, A-02, A-14) ──────────────────────
check('an Engineer cannot execute against Production (A-02)',
  !every(['EN'], 'run.execute', { environment: 'PROD' }).allow
  && !every(['EN'], 'sql.write', { environment: 'PROD' }).allow);
check('a Migration Lead can, and it is flagged high severity',
  (() => { const d = every(['ML'], 'run.execute', { environment: 'PROD', mutating: true });
           return d.allow && d.severity === 'high' && d.guardrail === 'production'; })());
check('unclassified defaults to PROD — the safe default (R-15/A-13)',
  !every(['EN'], 'run.execute', {}).allow
  && every(['EN'], 'run.execute', { environment: 'DEV' }).allow);
check('restricted Staging behaves as Production for an Engineer (6.3)',
  !every(['EN'], 'run.execute', { environment: 'STAGING' }).allow
  && every(['EN'], 'run.execute', { environment: 'STAGING', restrictedStaging: false }).allow);
check('Production reads are permitted but marked logged (6.2)',
  (() => { const d = every(['EN'], 'sql.read', { environment: 'PROD' });
           return d.allow && d.logged === true; })());
check('an Engineer cannot edit a Production connection (6.1)',
  !every(['EN'], 'connection.edit', { environment: 'PROD' }).allow
  && every(['EN'], 'connection.edit', { environment: 'DEV' }).allow);
check('only the PA classifies connections (A-14)',
  every(['PA'], 'connection.classify', {}).allow
  && ['OW','ML','EN','AP','DO','VA','AU','MB'].every(c => !every([c], 'connection.classify', {}).allow));

// ── Engineer limits (6.3 notes) ───────────────────────────────────────────
check('an Engineer cancels only their own runs, never on Production',
  every(['EN'], 'run.cancel', { environment: 'TEST', own: true }).allow
  && !every(['EN'], 'run.cancel', { environment: 'TEST', own: false }).allow
  && !every(['EN'], 'run.cancel', { environment: 'PROD', own: true }).allow);
check('an Engineer may author a schedule but not enable one',
  every(['EN'], 'schedule.create', { environment: 'DEV' }).allow
  && !every(['EN'], 'schedule.create', { environment: 'DEV', enable: true }).allow
  && !every(['EN'], 'schedule.enable', { environment: 'DEV' }).allow
  && every(['ML'], 'schedule.enable', { environment: 'DEV' }).allow);

// ── Auditor: sees everything, changes nothing (A-22) ─────────────────────
check('the Auditor reads the full audit log',
  (() => { const d = every(['AU'], 'audit.read', {}); return d.allow && d.grant === 'F'; })());
check('and holds no mutating grant anywhere in the matrix',
  Object.keys(R.MATRIX).every(a => {
    const d = R.can({ roles: ['AU'] }, a, { environment: 'DEV', mutating: true });
    return !d.allow || a === 'audit.read' || a === 'audit.export';
  }));
check('delivery roles read their own audit trail only (6.5)',
  (() => { const d = every(['EN'], 'audit.read', {}); return d.allow && d.selfOnly === true; })());
check('audit export is Auditor and Platform Administrator only',
  every(['AU'], 'audit.export', {}).allow && every(['PA'], 'audit.export', {}).allow
  && ['OW','ML','EN','AP','DO','VA','MB'].every(c => !every([c], 'audit.export', {}).allow));

// ── Role assignment rules (6.6) ───────────────────────────────────────────
check('a PA assigns delivery roles',
  R.validateAssignmentChange({ actorRoles: ['PA'], op: 'assign', targetRole: 'EN' }).ok);
check('but appointing a Platform Administrator requires the Owner',
  !R.validateAssignmentChange({ actorRoles: ['PA'], op: 'assign', targetRole: 'PA' }).ok
  && R.validateAssignmentChange({ actorRoles: ['OW'], op: 'assign', targetRole: 'PA' }).ok);
check('the Automation Principal is not assignable to a person',
  !R.validateAssignmentChange({ actorRoles: ['OW'], op: 'assign', targetRole: 'SP' }).ok);
check('the last administrator cannot be removed (lockout guard)',
  !R.validateAssignmentChange({ actorRoles: ['OW'], op: 'revoke', targetRole: 'PA', remainingAdmins: 0 }).ok
  && R.validateAssignmentChange({ actorRoles: ['OW'], op: 'revoke', targetRole: 'PA', remainingAdmins: 1 }).ok);
check('delivery roles cannot assign anything',
  ['ML','EN','AP','DO','VA','AU','MB'].every(c =>
    !R.validateAssignmentChange({ actorRoles: [c], op: 'assign', targetRole: 'EN' }).ok));

// ── SoD evaluators (5.2) ──────────────────────────────────────────────────
check('SoD-1: the author cannot approve their own change (A-03)',
  R.sodViolations('approve', { actorOid: 'u1', authorChain: ['u1', 'u2'] }).some(v => v.rule === 'SoD-1')
  && R.sodViolations('approve', { actorOid: 'u3', authorChain: ['u1', 'u2'] }).length === 0);
check('SoD-2: the sole approver cannot execute (A-04)',
  R.sodViolations('execute', { actorOid: 'u1', approvers: ['u1'] }).some(v => v.rule === 'SoD-2')
  && R.sodViolations('execute', { actorOid: 'u1', approvers: ['u1', 'u2'] }).length === 0);
check('SoD-7: holding Engineer and Approver together is surfaced (A-21)',
  R.collapsedSegregation(['EN', 'AP']).length === 1
  && R.collapsedSegregation(['EN', 'VA']).length === 0
  && R.collapsedSegregation(R.BOOTSTRAP_ROLES).length >= 2);

// ── Entra claims (Section 9) ──────────────────────────────────────────────
check('token app roles map to role codes, unknown values ignored',
  JSON.stringify(R.rolesFromClaims({ roles: ['Cygenix.Engineer', 'Cygenix.Auditor', 'Bogus.Role'] }))
  === JSON.stringify(['EN', 'AU']));
check('effective roles union stored and token grants',
  JSON.stringify(R.effectiveRoles(['ML'], ['EN', 'ML'], { isActive: true }).sort())
  === JSON.stringify(['EN', 'ML']));
check('a deactivated user has no effective roles, whatever the token says (R-04/A-18)',
  R.effectiveRoles(['OW', 'PA'], ['Cygenix.OrgOwner'], { isActive: false }).length === 0);

// ── Destructive-statement detection (R-17) ────────────────────────────────
const dd = R.detectDestructive;
check('an unbounded DELETE is a finding — the demo-group case (Section 7.2)',
  (() => { const f = dd('delete addresses; delete cases;');
           return f.length === 2 && f.every(x => x.type === 'DELETE' && !x.bounded); })());
check('a bounded DELETE is listed as bounded',
  (() => { const f = dd("DELETE FROM dbo.addresses WHERE id = 4");
           return f.length === 1 && f[0].bounded === true; })());
check('TRUNCATE and DROP are always findings',
  dd('TRUNCATE TABLE dbo.big')[0].type === 'TRUNCATE'
  && dd('DROP TABLE IF EXISTS dbo.big')[0].type === 'DROP TABLE');
check('an UPDATE without a predicate is a finding; with one it is bounded',
  dd('UPDATE t SET x = 1')[0].bounded === false
  && dd('UPDATE t SET x = 1 WHERE id = 2')[0].bounded === true);
check('the word DELETE inside a string or comment is not a finding',
  dd("INSERT INTO log(msg) VALUES ('please delete this later')").length === 0
  && dd('-- delete everything\nSELECT 1').length === 0);
check('INSERT alone is not flagged destructive — approval scope is deletes, drops and unbounded updates',
  dd('INSERT INTO t (a) VALUES (1)').length === 0);

// ── Read/write classification ─────────────────────────────────────────────
const ro = R.isReadOnlySql;
check('plain SELECT and CTE reads classify as read-only',
  ro('SELECT * FROM t WHERE x = 1') && ro('WITH c AS (SELECT 1 AS n) SELECT * FROM c'));
check('a CTE feeding an INSERT is a write',
  !ro('WITH c AS (SELECT 1 AS n) INSERT INTO t SELECT * FROM c'));
check('SELECT ... INTO is a write', !ro('SELECT * INTO t2 FROM t'));
check('DML, DDL and EXEC are writes',
  !ro('DELETE FROM t') && !ro('DROP TABLE t') && !ro('EXEC sp_who'));
check('a column merely named update_date does not make a read a write',
  ro('SELECT update_date FROM t'));

// ── Audit hash chain (Section 10 / A-09) ──────────────────────────────────
const e1 = R.chainEntry('', { seq: 1, action: 'x', outcome: 'allowed' });
const e2 = R.chainEntry(e1.entryHash, { seq: 2, action: 'y', outcome: 'denied' });
const e3 = R.chainEntry(e2.entryHash, { seq: 3, action: 'z', outcome: 'allowed' });
check('an intact chain verifies', R.verifyEntries([e1, e2, e3]).ok);
check('excising an entry breaks the chain and names the break (A-09)',
  (() => { const v = R.verifyEntries([e1, e3]); return !v.ok && v.brokenAt === 3; })());
check('tampering with an entry breaks its own hash',
  (() => { const t = { ...e2, action: 'y-edited' };
           return !R.verifyEntries([e1, t, e3]).ok; })());

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
