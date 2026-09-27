// tests/drive-store.test.js — the Assistant's door to the Drive, in Node.
//
// WHY THIS FILE EXISTS
//
// public/cygenix-drive-store.js decides two things that must be right before
// the Assistant is allowed near a user's files: WHERE it may write (only
// inside Claude/<active project>/, judged from the resolved parent chain and
// never from the path string) and WHAT it may read (nothing that looks like
// a credential, by path or by content). Both are pure rules over a node
// tree, so they are exercised here against an in-memory IndexedDB with no
// browser, no Blob and no sync engine — the module takes all three as
// injected dependencies for exactly this reason.
//
// The history rule is here too: an overwrite or delete must keep the old
// content, and the eleventh version must drop the oldest. That is what makes
// "the Assistant cannot destroy anything" true rather than hoped.

'use strict';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));
const rejects = async (p, code) => {
  try { await p; return { ok: false, got: 'resolved' }; }
  catch (e) { return { ok: !code || e.code === code, got: e.code + ': ' + e.message }; }
};

const M = require('../public/cygenix-drive-store.js');

/* An in-memory stand-in for the "nodes" store. Content is kept as a plain
   string, which the module's injected readContent/makeContent understand. */
function world(opts) {
  opts = opts || {};
  const nodes = new Map();
  let t = 1_700_000_000_000, n = 0;
  const idb = {
    get: async (id) => (nodes.has(id) ? Object.assign({}, nodes.get(id)) : null),
    put: async (node) => { nodes.set(node.id, Object.assign({}, node)); return node; },
    del: async (id) => { nodes.delete(id); },
    children: async (pid) => [...nodes.values()].filter((x) => (x.parentId || '') === (pid || '')).map((x) => Object.assign({}, x)),
  };
  const synced = [];
  const store = M.create({
    idb,
    now: () => (t += 1000),
    uid: () => 'n' + (++n),
    makeContent: (text) => String(text),
    readContent: async (c) => String(c == null ? '' : c),
    sync: async () => { synced.push(1); return { ok: true }; },
  });
  // Seed helpers that write straight into the tree, as a UI would.
  const folder = async (parentId, name, meta) => { const f = { id: 'u' + (++n), parentId: parentId || '', name, kind: 'folder', mtime: (t += 1000) }; if (meta) f.meta = meta; await idb.put(f); return f; };
  const file = async (parentId, name, text, meta) => { const f = { id: 'u' + (++n), parentId: parentId || '', name, kind: 'file', size: text.length, mime: 'text/plain', mtime: (t += 1000), content: text }; if (meta) f.meta = meta; await idb.put(f); return f; };
  return { store, idb, nodes, folder, file, synced, all: () => [...nodes.values()] };
}

(async () => {
  console.log('Drive store — where the Assistant may write, what it may read\n');

  /* ── 1. Paths ──────────────────────────────────────────────────────── */
  section('1. Path rules');
  check('a plain relative path splits', JSON.stringify(M.splitPath('Claude/Demo/scripts/load.sql')) === '["Claude","Demo","scripts","load.sql"]');
  check('the root is the empty path', M.splitPath('').length === 0 && M.splitPath(null).length === 0);
  check('".." is refused', M.splitPath('Claude/../x') === null && M.splitPath('..') === null);
  check('a leading slash is refused', M.splitPath('/Claude/Demo') === null);
  check('a backslash is refused', M.splitPath('Claude\\Demo') === null);
  check('an empty segment is refused', M.splitPath('Claude//Demo') === null);

  {
    const w = world();
    const a = await w.folder('', 'Docs');
    await w.file(a.id, 'Readme.md', 'hello');
    const r = await w.store.resolve('docs/readme.md');
    check('matching is case-insensitive', r && r.node && r.node.name === 'Readme.md' && r.missing.length === 0);
    await w.file(a.id, 'README.md', 'other');
    const amb = await rejects(w.store.resolve('docs/readme.md'), 'ambiguous');
    check('two siblings differing only in case is an error, not a guess', amb.ok, amb.got);
  }

  /* ── 2. The workspace ──────────────────────────────────────────────── */
  section('2. ensureWorkspace');
  {
    const w = world();
    const ws = await w.store.ensureWorkspace('p1', 'Demo');
    check('creates Claude/ marked reserved', ws.root.meta.reserved === 'claude' && ws.root.parentId === '');
    check('and Claude/Demo marked with the project id', ws.project.meta.claudeProject === 'p1' && ws.project.parentId === ws.root.id && ws.path === 'Claude/Demo');
    const kids = await w.idb.children(ws.project.id);
    const names = kids.map((k) => k.kind + ':' + k.name).sort();
    check('with rules.md, notes.md, scripts/ and results/', JSON.stringify(names) === '["file:notes.md","file:rules.md","folder:results","folder:scripts"]', names.join(','));
    const rules = await w.store.readText('Claude/Demo/rules.md');
    check('rules.md is a template of the three headings and nothing invented',
      /## How to write scripts/.test(rules.text) && /## Where things may run/.test(rules.text) && /## Things never to do/.test(rules.text)
      && !/never|always/i.test(rules.text.replace(/Things never to do/, '')), rules.text);
    const notes = await w.store.readText('Claude/Demo/notes.md');
    check('notes.md starts as a one-line header', notes.text.trim() === "# Assistant's notes");
    const again = await w.store.ensureWorkspace('p1', 'Demo');
    check('calling it again creates nothing new', again.project.id === ws.project.id && w.all().length === 6, w.all().length);
    const renamed = await w.store.ensureWorkspace('p1', 'Demo Migration');
    check('a renamed project renames the folder, found by id not name', renamed.project.id === ws.project.id && renamed.project.name === 'Demo Migration' && renamed.path === 'Claude/Demo Migration');
  }
  {
    const w = world();
    await w.folder('', 'Claude');                       // the user's own folder, unmarked
    const ws = await w.store.ensureWorkspace('p1', 'Demo');
    check("a user's own folder called Claude is left alone; the reserved one takes another name",
      ws.root.name === 'Claude (2)' && ws.root.meta.reserved === 'claude');
  }

  /* ── 3. The write guard ────────────────────────────────────────────── */
  section('3. Where it may write');
  {
    const w = world();
    const ws = await w.store.ensureWorkspace('p1', 'Demo');
    const other = await w.folder('', 'Other');
    await w.file(other.id, 'x.sql', 'select 1');
    const ok = await w.store.writeText('Claude/Demo/scripts/load.sql', 'select 1', { projectId: 'p1' });
    check('a write inside Claude/<project>/ succeeds and syncs', ok.path === 'Claude/Demo/scripts/load.sql' && w.synced.length === 1, JSON.stringify(ok));
    const deep = await w.store.writeText('Claude/Demo/results/2026/q3/out.csv', 'a,b', { projectId: 'p1' });
    check('missing folders inside the workspace are created on the way', deep.path === 'Claude/Demo/results/2026/q3/out.csv');
    for (const [p, why] of [
      ['Other/x.sql', 'another folder'], ['x.sql', 'the root'], ['Claude/x.sql', 'the reserved root itself'],
      ['Claude/Demo/../../Other/y.sql', '".."'], ['/Claude/Demo/y.sql', 'a leading slash'], ['Claude\\Demo\\y.sql', 'a backslash'],
    ]) {
      const r = await rejects(w.store.writeText(p, 'x', { projectId: 'p1' }), 'outside-workspace');
      check('refused: ' + why + ' (' + p + ')', r.ok, r.got);
    }
    // A folder named like the workspace, elsewhere: the chain does not reach the reserved root.
    const fake = await w.folder(other.id, 'Claude');
    const fakeP = await w.folder(fake.id, 'Demo', { claudeProject: 'p1' });
    const r1 = await rejects(w.store.writeText('Other/Claude/Demo/evil.sql', 'x', { projectId: 'p1' }), 'outside-workspace');
    check('a folder named like the workspace elsewhere is refused — the chain, not the string, decides', r1.ok, r1.got);
    // The project folder moved out from under the reserved root.
    const ws2 = await w.store.ensureWorkspace('p2', 'Second');
    const moved = await w.idb.get(ws2.project.id); moved.parentId = other.id; await w.idb.put(moved);
    const r2 = await rejects(w.store.writeText('Other/Second/a.sql', 'x', { projectId: 'p2' }), 'outside-workspace');
    check('a project folder the user moved out of Claude/ is refused for that project', r2.ok, r2.got);
    const r3 = await rejects(w.store.writeText('Claude/Demo/scripts/load.sql', 'x', { projectId: 'p2' }), 'outside-workspace');
    check("another project's workspace is refused", r3.ok, r3.got);
    const r4 = await rejects(w.store.writeText('claude/demo/scripts/Second.sql', 'x', { projectId: 'p1' }));
    check('the workspace path matches case-insensitively', !r4.ok, r4.got);
    check('reads are not fenced: another folder reads fine', (await w.store.readText('Other/x.sql')).text === 'select 1');
    void fakeP;
  }

  /* ── 4. History ────────────────────────────────────────────────────── */
  section('4. Nothing is destroyed');
  {
    const w = world();
    await w.store.ensureWorkspace('p1', 'Demo');
    await w.store.writeText('Claude/Demo/scripts/a.sql', 'v1', { projectId: 'p1' });
    const dup = await rejects(w.store.writeText('Claude/Demo/scripts/a.sql', 'v2', { projectId: 'p1' }), 'exists');
    check('create refuses to replace an existing file', dup.ok, dup.got);
    const ow = await w.store.writeText('Claude/Demo/scripts/a.sql', 'v2', { projectId: 'p1', mode: 'overwrite' });
    check('overwrite replaces it', ow.replaced && (await w.store.readText('Claude/Demo/scripts/a.sql')).text === 'v2');
    const hist = await w.store.list('Claude/Demo/.history');
    check('and keeps the old content in .history/ under a stamped name',
      hist.entries.length === 1 && /^a\.sql\.\d{8}-\d{6}$/.test(hist.entries[0].name)
      && (await w.store.readText('Claude/Demo/.history/' + hist.entries[0].name)).text === 'v1', JSON.stringify(hist.entries));
    for (let i = 3; i <= 12; i++) await w.store.writeText('Claude/Demo/scripts/a.sql', 'v' + i, { projectId: 'p1', mode: 'overwrite' });
    const h2 = await w.store.list('Claude/Demo/.history');
    const kept = [];
    for (const e of h2.entries) kept.push((await w.store.readText('Claude/Demo/.history/' + e.name)).text);
    kept.sort((a, b) => Number(a.slice(1)) - Number(b.slice(1)));
    check('eleven versions later, exactly ten are kept and v1 (the oldest) is gone',
      h2.entries.length === 10 && kept[0] === 'v2' && kept[9] === 'v11', kept.join(','));
    const rm = await w.store.remove('Claude/Demo/scripts/a.sql', { projectId: 'p1' });
    const gone = await rejects(w.store.readText('Claude/Demo/scripts/a.sql'), 'not-found');
    check('delete moves the file into .history/ rather than destroying it',
      gone.ok && /^Claude\/Demo\/\.history\/a\.sql\./.test(rm.movedTo)
      && (await w.store.readText(rm.movedTo)).text === 'v12', rm.movedTo);
    const rmOut = await rejects(w.store.remove('Claude/Demo/rules.md', { projectId: 'p2' }), 'outside-workspace');
    check('delete obeys the same fence', rmOut.ok, rmOut.got);
  }

  /* ── 5. The secret guard ───────────────────────────────────────────── */
  section('5. What it refuses to read — by path');
  for (const f of M.SECRET_PATHS.folders) {
    check('folder "' + f + '" is blocked wherever it sits', M.isBlockedPath('a/' + f + '/b.txt') && M.isBlockedPath(f + '/x') && M.isBlockedPath('a/' + f));
  }
  for (const [name, ex] of [['local.settings.json', 'local.settings.json'], ['.env', '.env'], ['.env.*', '.env.production'],
    ['*.pem', 'server.pem'], ['*.pfx', 'cert.pfx'], ['*.key', 'private.key'], ['id_rsa*', 'id_rsa.pub'], ['*.publishsettings', 'site.publishsettings']]) {
    check('file pattern ' + name + ' is blocked (' + ex + ')', M.isBlockedPath('some/folder/' + ex));
  }
  check('SECRET_PATHS.files has one entry per pattern the brief names', M.SECRET_PATHS.files.length === 8);
  check('an ordinary path is not blocked', !M.isBlockedPath('Claude/Demo/scripts/load.sql') && !M.isBlockedPath('notes/keys-to-success.md'));

  section('6. What it refuses to read — by content');
  for (const [name, sample] of [
    ['connection-string password', 'Server=x;Database=y;User Id=u;Password=Hunter2;'],
    ['connection-string password', 'Server=x;Pwd=abc;'],
    ['connection-string password', 'DefaultEndpointsProtocol=https;AccountName=a;AccountKey=abc123==;'],
    ['Anthropic API key', 'key: sk-ant-api03-xxxx'],
    ['GitHub token', 'token ghp_abcdef123456'],
    ['GitHub token', 'github_pat_11AAAAAA_bbbbbb'],
    ['private key', '-----BEGIN RSA PRIVATE KEY-----\nMIIE'],
    ['shared access signature', 'SharedAccessSignature sr=https://x'],
    ['client secret', '{"client_secret": "abc"}'],
  ]) {
    check('"' + sample.slice(0, 40) + '" is refused as a ' + name, M.findSecret(sample) === name, M.findSecret(sample));
  }
  check('every SECRET_CONTENT pattern has been hit above', M.SECRET_CONTENT.length === 6);
  check('near miss: a SQL comment mentioning the word password passes', M.findSecret('-- reset the password before the run\nSELECT 1') === null);
  check('near miss: connection.md without credentials passes', M.findSecret('# connection\nServer: db.example.com\nDatabase: Sales\nAuth: managed identity') === null);
  {
    const w = world();
    const cfg = await w.folder('', 'config');
    await w.file(cfg.id, 'app.txt', 'Server=x;Password=abc;');
    await w.file(cfg.id, '.env', 'X=1');
    await w.file(cfg.id, 'ok.md', 'nothing here');
    const r = await rejects(w.store.readText('config/app.txt'), 'secret-content');
    check('a read that hits a content pattern is refused and names the file and the reason',
      r.ok && /config\/app\.txt/.test(r.got) && /password/i.test(r.got), r.got);
    const r2 = await rejects(w.store.readText('config/.env'), 'secret-path');
    check('a read of a blocked path is refused before anything is read', r2.ok, r2.got);
    const l = await w.store.list('config');
    check('drive_list hides the blocked path entirely', l.entries.map((e) => e.name).join(',') === 'app.txt,ok.md', JSON.stringify(l.entries.map((e) => e.name)));
    const s = await w.store.search('=');
    check('drive_search never surfaces a line from a file with a secret in it', s.hits.every((h) => h.path !== 'config/app.txt' && h.path !== 'config/.env'), JSON.stringify(s.hits));
  }

  /* ── 7. Reads ──────────────────────────────────────────────────────── */
  section('7. Reading and listing');
  {
    const w = world();
    const d = await w.folder('', 'Data');
    await w.file(d.id, 'big.txt', 'x'.repeat(1000));
    await w.file(d.id, 'image.png', 'PNG');
    const r = await w.store.readText('Data/big.txt', { maxChars: 100 });
    check('a long read is cut where asked and says so', r.truncated && r.text.length === 100 && /Cut at 100/.test(r.note));
    const p = await w.store.readText('Data/image.png');
    check('a non-text file returns its size and is not readable yet', p.readable === false && p.size === 3 && /not readable/.test(p.note));
    const nf = await rejects(w.store.readText('Data/missing.txt'), 'not-found');
    check('a missing file is not-found', nf.ok, nf.got);
    for (let i = 0; i < 600; i++) await w.file(d.id, 'f' + i + '.txt', 'x');
    const l = await w.store.list('', { recursive: true });
    check('a recursive listing is capped at 500 and says it was cut', l.entries.length === 500 && l.truncated && /cut/i.test(l.note));
    const s = await w.store.search('x');
    check('a search is capped at 50 hits and says so', s.hits.length === 50 && s.truncated);
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
