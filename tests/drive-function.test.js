// tests/drive-function.test.js — drive.js refuses an Assistant's write outside its folder.
//
// WHY THIS FILE EXISTS
//
// The Assistant runs in the browser under the user's own token, so the Drive
// function cannot tell it apart from the user. public/cygenix-drive-store.js
// fences it client-side; this is the defence in depth behind that fence. The
// client marks its syncs `x-cygenix-actor: assistant`, and under that header
// every write target's parentId chain has to reach the reserved folder.
//
// What this proves, against a faked blob store and a faked authorizer:
//   · a marked write outside the workspace is refused with 403;
//   · the same write with no header is accepted exactly as before;
//   · a marked delete inside the workspace is allowed, of the reserved
//     folder itself is not, and of a node the manifest does not know is not;
//   · content arriving before its metadata is judged by the parent it names;
//   · a folder claiming to be the reserved one anywhere but the top level,
//     or as a second one, is refused.

'use strict';

const path = require('path');
const Module = require('module');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const FN_DIR = path.join(__dirname, '..', 'netlify', 'functions');
const MEM = new Map();
const BLOBS = {
  get: async (k) => (MEM.has(k) ? JSON.parse(MEM.get(k)) : null),
  setJSON: async (k, v) => { MEM.set(k, JSON.stringify(v)); },
  set: async (k) => { MEM.set(k, '"bytes"'); },
  getWithMetadata: async () => null,
  delete: async (k) => { MEM.delete(k); },
};
const realRequire = Module.prototype.require;
Module.prototype.require = function (id) {
  if (id === '@netlify/blobs') return { getStore: () => BLOBS };
  if (id === './lib/authz') return {
    authorize: async () => ({ authed: { sub: 'user-1' }, actor: { oid: 'user-1' } }),
    AuthzError: class extends Error { constructor(m, s) { super(m); this.statusCode = s; } },
    errorResponse: (e, cors) => ({ statusCode: e.statusCode || 401, headers: cors, body: JSON.stringify({ error: e.message }) }),
  };
  return realRequire.call(this, id);
};
const { handler } = require(path.join(FN_DIR, 'drive.js'));

async function call(body, headers) {
  const res = await handler({ httpMethod: 'POST', headers: Object.assign({ authorization: 'Bearer t' }, headers || {}), body: JSON.stringify(body) });
  let json = null; try { json = JSON.parse(res.body); } catch { /* raw */ }
  return { status: res.statusCode, json };
}
const asAssistant = { 'x-cygenix-actor': 'assistant' };
const node = (id, parentId, name, kind, meta) => Object.assign({ id, parentId, name, kind: kind || 'file', mtime: 1, size: 1 }, meta ? { meta } : {});

(async () => {
  console.log('Drive function — the Assistant header\n');

  // A Drive with the user's own folder and the reserved workspace.
  MEM.clear();
  const seed = [
    node('claude', '', 'Claude', 'folder', { reserved: 'claude' }),
    node('proj', 'claude', 'Demo', 'folder', { claudeProject: 'p1' }),
    node('scripts', 'proj', 'scripts', 'folder'),
    node('rules', 'proj', 'rules.md'),
    node('other', '', 'Other', 'folder'),
    node('theirs', 'other', 'theirs.sql'),
  ];
  let r = await call({ action: 'put-meta', nodes: seed });
  check('the seed goes in without a header, as every ordinary sync does', r.status === 200 && r.json.count === 6, JSON.stringify(r));

  // ── put-meta ──
  r = await call({ action: 'put-meta', nodes: [node('evil', 'other', 'evil.sql')] }, asAssistant);
  check('an Assistant-marked write outside the workspace is refused with 403', r.status === 403 && /reserved folder/.test(r.json.error), JSON.stringify(r));
  r = await call({ action: 'put-meta', nodes: [node('evil', 'other', 'evil.sql')] });
  check('the same write with no header is accepted exactly as before', r.status === 200 && r.json.count === 1, JSON.stringify(r));
  r = await call({ action: 'put-meta', nodes: [node('load', 'scripts', 'load.sql')] }, asAssistant);
  check('a marked write inside Claude/Demo/scripts is allowed', r.status === 200, JSON.stringify(r));
  r = await call({ action: 'put-meta', nodes: [node('newdir', 'proj', 'results', 'folder'), node('out', 'newdir', 'out.csv')] }, asAssistant);
  check('a new folder and a file inside it, arriving in one batch, are judged together', r.status === 200 && r.json.count === 2, JSON.stringify(r));
  r = await call({ action: 'put-meta', nodes: [node('top', '', 'top.txt')] }, asAssistant);
  check('a marked write at the Drive root is refused', r.status === 403);
  r = await call({ action: 'put-meta', nodes: [node('fake', 'other', 'Claude', 'folder', { reserved: 'claude' })] }, asAssistant);
  check('a folder claiming to be reserved below the top level is refused', r.status === 403 && /reserved folder there/.test(r.json.error), JSON.stringify(r));
  r = await call({ action: 'put-meta', nodes: [node('second', '', 'Claude 2', 'folder', { reserved: 'claude' })] }, asAssistant);
  check('a second reserved folder is refused', r.status === 403);
  MEM.clear();
  r = await call({ action: 'put-meta', nodes: [node('claude', '', 'Claude', 'folder', { reserved: 'claude' }), node('proj', 'claude', 'Demo', 'folder')] }, asAssistant);
  check('on an empty Drive the Assistant may create the reserved folder itself, at the top', r.status === 200 && r.json.count === 2, JSON.stringify(r));
  await call({ action: 'put-meta', nodes: seed });

  // ── put-content ──
  r = await call({ action: 'put-content', id: 'new1', contentB64: 'YQ==', parentId: 'scripts' }, asAssistant);
  check('marked content naming a parent inside the workspace is accepted before its metadata exists', r.status === 200, JSON.stringify(r));
  r = await call({ action: 'put-content', id: 'new2', contentB64: 'YQ==', parentId: 'other' }, asAssistant);
  check('marked content naming a parent outside is refused', r.status === 403);
  r = await call({ action: 'put-content', id: 'new3', contentB64: 'YQ==' }, asAssistant);
  check('marked content naming no parent is refused', r.status === 403);
  r = await call({ action: 'put-content', id: 'new4', contentB64: 'YQ==' });
  check('unmarked content needs no parent, as before', r.status === 200);

  // ── delete ──
  r = await call({ action: 'delete', ids: ['theirs'] }, asAssistant);
  check('a marked delete outside the workspace is refused', r.status === 403);
  r = await call({ action: 'delete', ids: ['rules'] }, asAssistant);
  check('a marked delete inside the workspace is allowed', r.status === 200 && r.json.deleted === 1, JSON.stringify(r));
  r = await call({ action: 'delete', ids: ['claude'] }, asAssistant);
  check('the Assistant may never delete the reserved folder itself', r.status === 403);
  r = await call({ action: 'delete', ids: ['proj'] }, asAssistant);
  check('but may delete a project folder inside it', r.status === 200);
  r = await call({ action: 'delete', ids: ['ghost'] }, asAssistant);
  check('a marked delete of a node the manifest does not know is refused rather than guessed', r.status === 403);
  r = await call({ action: 'delete', ids: ['theirs'] });
  check('an unmarked delete anywhere is accepted, as before', r.status === 200 && r.json.deleted === 1);

  // ── reads never care ──
  r = await call({ action: 'manifest' }, asAssistant);
  check('a marked read is an ordinary read', r.status === 200 && r.json.manifest);
  r = await call({ action: 'put-meta', nodes: [node('x', 'other', 'x')] }, { 'x-cygenix-actor': 'ASSISTANT' });
  check('the header is matched without regard to case', r.status === 403);

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
