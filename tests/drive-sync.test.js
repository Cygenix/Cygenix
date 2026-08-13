// Tests the Drive sync three-way merge by loading the real module with
// minimal browser stubs, so the logic under test is the shipped code.
const fs = require('fs');
const vm = require('vm');

const listeners = {};
const sandbox = {
  window: {
    addEventListener: (k, f) => { (listeners[k] = listeners[k] || []).push(f); },
    dispatchEvent: () => true,
    location: { origin: 'https://x' },
  },
  document: { hidden: false, addEventListener: () => {} },
  indexedDB: { open: () => ({}) },
  localStorage: { getItem: () => null, setItem: () => {}, removeItem: () => {} },
  sessionStorage: { getItem: () => null },
  setTimeout: () => 0, setInterval: () => 0, clearTimeout: () => {},
  console, CustomEvent: function () {}, fetch: () => Promise.reject(new Error('no net')),
  Blob: function () {}, btoa: () => '', atob: () => '', AbortSignal: { timeout: () => null },
  crypto: { randomUUID: () => 'x' },
};
sandbox.window.window = sandbox.window;
vm.createContext(sandbox);
vm.runInContext(fs.readFileSync('' + __dirname + '/../public/cygenix-drive-sync.js', 'utf8'), sandbox);

const plan = sandbox.window.CygenixDriveSync._plan;
const n = (id, mtime) => ({ id, mtime, kind: 'file', name: id });

let pass = 0, fail = 0;
function check(label, got, want) {
  const g = JSON.stringify(got), w = JSON.stringify(want);
  if (g === w) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + '\n        got  ' + g + '\n        want ' + w); }
}
const ids = a => a.map(x => (typeof x === 'string' ? x : x.id)).sort();

console.log('Drive sync — three-way merge\n');

// 1. The user's actual complaint: files uploaded on machine A, machine B empty.
{
  const r = plan({}, { a: n('a', 100), b: n('b', 100) }, {});
  check('new machine downloads everything from the account',
    ids(r.toDownload), ['a', 'b']);
  check('  ...and deletes nothing', [r.toDeleteLocal.length, r.toDeleteRemote.length], [0, 0]);
}

// 2. First upload from a machine that has local files and an empty cloud.
{
  const r = plan({ a: n('a', 100) }, {}, {});
  check('local-only file with no baseline uploads', ids(r.toUpload), ['a']);
  check('  ...and is NOT deleted', r.toDeleteLocal.length, 0);
}

// 3. Delete propagation — the case that breaks naive last-write-wins.
{
  const r = plan({ a: n('a', 100) }, {}, { a: 100 });
  check('file deleted on another machine is removed here', ids(r.toDeleteLocal), ['a']);
  check('  ...and is NOT re-uploaded (no resurrection)', r.toUpload.length, 0);
}
{
  const r = plan({}, { a: n('a', 100) }, { a: 100 });
  check('file deleted here is deleted in the cloud', ids(r.toDeleteRemote), ['a']);
  check('  ...and is NOT re-downloaded (no resurrection)', r.toDownload.length, 0);
}

// 4. Edits on both sides — newer wins.
check('newer local edit uploads',
  ids(plan({ a: n('a', 200) }, { a: n('a', 100) }, { a: 100 }).toUpload), ['a']);
check('newer remote edit downloads',
  ids(plan({ a: n('a', 100) }, { a: n('a', 200) }, { a: 100 }).toDownload), ['a']);
{
  const r = plan({ a: n('a', 100) }, { a: n('a', 100) }, { a: 100 });
  check('unchanged file does nothing',
    [r.toUpload.length, r.toDownload.length, r.toDeleteLocal.length, r.toDeleteRemote.length],
    [0, 0, 0, 0]);
}

// 5. A stale baseline entry for a file gone from both sides must not act.
{
  const r = plan({}, {}, { gone: 100 });
  check('baseline-only entry triggers no action',
    [r.toUpload.length, r.toDownload.length, r.toDeleteLocal.length, r.toDeleteRemote.length],
    [0, 0, 0, 0]);
}

// 6. Fresh install with an existing cloud Drive must never delete the cloud.
//    (Empty baseline + empty local + populated remote = pure download.)
{
  const r = plan({}, { a: n('a', 1), b: n('b', 2), c: n('c', 3) }, {});
  check('signing in on a new PC never deletes cloud files', r.toDeleteRemote.length, 0);
  check('  ...it downloads all of them', ids(r.toDownload), ['a', 'b', 'c']);
}

// 7. Mixed real-world round: one new here, one new there, one deleted there.
{
  const r = plan(
    { keep: n('keep', 50), mine: n('mine', 90), goner: n('goner', 40) },
    { keep: n('keep', 50), theirs: n('theirs', 80) },
    { keep: 50, goner: 40 }
  );
  check('mixed: uploads local-new', ids(r.toUpload), ['mine']);
  check('mixed: downloads remote-new', ids(r.toDownload), ['theirs']);
  check('mixed: deletes locally what was removed elsewhere', ids(r.toDeleteLocal), ['goner']);
  check('mixed: leaves the unchanged file alone', r.toDeleteRemote.length, 0);
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
