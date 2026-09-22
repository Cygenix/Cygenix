// tests/fixtures/grant-role-stub.js — an in-memory authorisation store.
//
// Preloaded with `node -r` in front of scripts/grant-role.js so the script
// runs for real against a fake store instead of a live Netlify site. Only
// org-store is replaced; rbac.js is the genuine module, because the role
// codes and the conflicting-pair rules are exactly what the test is for.
//
// The seed arrives as JSON in GRANT_ROLE_SEED. Whatever the script leaves
// behind is printed as one __RESULT__ line, which the test parses.
'use strict';

const Module = require('module');
const path = require('path');

const seed = JSON.parse(process.env.GRANT_ROLE_SEED || '{}');
const state = {
  users: seed.users || {},
  assignments: seed.assignments || [],
  classifications: {},
};
const calls = { setJSON: [], audits: [], invalidated: 0 };

const fakeStore = {
  get: async () => null,
  setJSON: async (key, value) => {
    if (seed.failWrite) throw new Error('simulated blob write failure');
    calls.setJSON.push({ key, value });
    if (key === 'rbac/assignments') state.assignments = value.assignments;
  },
};

const fakeOrg = {
  orgStore: () => fakeStore,
  loadAll: async () => state,
  invalidate: () => { calls.invalidated++; },
  appendAudit: async (store, evt) => {
    if (seed.failAudit) throw new Error('simulated audit failure');
    calls.audits.push(evt);
    return { id: 'e1', seq: 1 };
  },
};

const ORG = path.join(__dirname, '..', '..', 'netlify', 'functions', 'lib', 'org-store.js');
const origLoad = Module._load;
Module._load = function (request, parent, isMain) {
  const resolved = (() => {
    try { return Module._resolveFilename(request, parent, isMain); }
    catch (e) { return request; }
  })();
  if (resolved === ORG) return fakeOrg;
  return origLoad.apply(this, arguments);
};

process.on('exit', () => {
  try {
    process.stdout.write('\n__RESULT__' + JSON.stringify({
      assignments: state.assignments,
      wroteKeys: calls.setJSON.map((c) => c.key),
      audits: calls.audits,
      invalidated: calls.invalidated,
    }) + '\n');
  } catch (e) { /* nothing useful to do in an exit handler */ }
});
