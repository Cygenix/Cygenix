// tests/sql-windows.test.js — the SQL Editor's window list.
//
// The editor was one query at a time. Comparing two result sets, or keeping a
// lookup query beside the one being written, meant retyping — so people kept
// the other query in a text file, which is the tell that the tool was missing
// something.
//
// This covers the LIST, not the editor: ordering, the close-and-reopen stack,
// the cap, dirty tracking and what survives a reload. Those are the parts with
// real edge cases, and they are testable without a browser only because the
// module owns no DOM. The editor half — that switching really restores undo
// history, the cursor and the cached results — needs a real Monaco and lives
// in tests/browser/sql-windows.smoke.js.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};
const P = (...p) => path.join(__dirname, '..', ...p);
const read = (...p) => fs.readFileSync(P(...p), 'utf8');

const W = require('../public/cygenix-sql-windows.js');

/* A storage stand-in, so persistence is exercised rather than described. */
function fakeStorage() {
  const map = {};
  return {
    getItem: (k) => (Object.prototype.hasOwnProperty.call(map, k) ? map[k] : null),
    setItem: (k, v) => { map[k] = String(v); },
    removeItem: (k) => { delete map[k]; },
    __map: map,
  };
}
const store = (s, p) => W.makeStore({ storage: s || fakeStorage(), projectId: p || 'proj1' });

console.log('SQL Editor windows — the list, its order, and what survives a reload\n');

/* ── 1. Opening and naming ───────────────────────────────────────────────── */

{
  const s = store();
  s.init();
  check('a fresh editor starts with exactly one window', s.list().length === 1);
  check('and it is the active one', s.activeId() === s.list()[0].id);
  check('named Query 1', s.list()[0].name === 'Query 1');

  s.create(); s.create();
  check('new windows are numbered on from there',
    s.list().map((w) => w.name).join(', ') === 'Query 1, Query 2, Query 3',
    s.list().map((w) => w.name).join(', '));
  check('and opening one makes it active', s.activeId() === s.list()[2].id);

  // Numbers must not be reused after a close, or two tabs end up with one name.
  s.close(s.list()[2].id);
  s.create();
  check('a closed number is not handed out again',
    s.list().map((w) => w.name).join(', ') === 'Query 1, Query 2, Query 4',
    s.list().map((w) => w.name).join(', '));
}

/* ── 2. The cap ──────────────────────────────────────────────────────────── */

{
  const s = store();
  s.init();
  let last;
  for (let i = 1; i < W.MAX_WINDOWS + 5; i++) last = s.create();
  check('the cap holds at ' + W.MAX_WINDOWS, s.list().length === W.MAX_WINDOWS, s.list().length);
  check('and refusing says what to do about it rather than failing silently',
    last.ok === false && /Close one you have finished with/.test(last.reason), last.reason);
}

/* ── 3. Closing, and what becomes active ─────────────────────────────────── */

{
  const s = store();
  s.init(); s.create(); s.create();               // 1, 2, 3
  const [a, b, c] = s.list().map((w) => w.id);
  s.activate(b);
  s.close(b);
  check('closing the active window activates its right-hand neighbour, as a browser does',
    s.activeId() === c, s.active().name);

  s.activate(c); s.close(c);
  check('and the left-hand one when there is nothing to the right', s.activeId() === a);

  s.close(a);
  check('closing the last window leaves a fresh empty one rather than nothing',
    s.list().length === 1 && s.list()[0].sql === '', JSON.stringify(s.list().map((w) => w.name)));

  const other = store();
  other.init(); other.create();
  const keep = other.list()[0].id;
  other.close(other.list()[1].id);
  check('closing a background window leaves the active one alone', other.activeId() === keep);
}

/* ── 4. Reopen ───────────────────────────────────────────────────────────── */

{
  const s = store();
  s.init();
  s.update(s.activeId(), { sql: 'SELECT 1' });
  s.create(); s.update(s.activeId(), { sql: 'SELECT 2' });
  s.create(); s.update(s.activeId(), { sql: 'SELECT 3' });
  const middle = s.list()[1].id;

  check('nothing to reopen before anything is closed', s.canReopen() === false);
  s.close(middle);
  check('there is now', s.canReopen() === true);
  const back = s.reopen();
  check('reopening restores the window, its text and its place in the strip',
    back.ok && s.list()[1].sql === 'SELECT 2' && s.list().length === 3,
    s.list().map((w) => w.sql).join(' | '));
  check('and makes it active', s.activeId() === s.list()[1].id);
  check('the stack empties as it is used', s.canReopen() === false);

  // Repeated closes come back in reverse, like a browser.
  const ids = s.list().map((w) => w.id);
  s.close(ids[0]); s.close(ids[2]);
  s.reopen();
  check('the most recently closed comes back first',
    s.list().some((w) => w.sql === 'SELECT 3'), s.list().map((w) => w.sql).join(' | '));
}

/* ── 5. Dirty tracking ───────────────────────────────────────────────────── */

{
  const s = store();
  s.init();
  const id = s.activeId();
  check('a new empty window is not dirty', s.isDirty(s.active()) === false);
  s.update(id, { sql: 'SELECT 1' });
  check('typing makes it dirty', s.isDirty(s.active()) === true);
  s.markSaved(id);
  check('saving clears it', s.isDirty(s.active()) === false);
  s.update(id, { sql: 'SELECT 2' });
  check('editing after a save makes it dirty again', s.isDirty(s.active()) === true);
  s.update(id, { sql: 'SELECT 1' });
  check('and typing back to exactly what was saved makes it clean',
    s.isDirty(s.active()) === false,
    'dirty is a comparison, not a flag that latches');

  s.create(); s.update(s.activeId(), { sql: 'unsaved' });
  check('the store can name which windows would lose work',
    s.anyDirty() === true && s.dirtyNames().length === 1,
    s.dirtyNames().join(', '));
}

/* ── 6. Reordering ───────────────────────────────────────────────────────── */

{
  const s = store();
  s.init(); s.create(); s.create();
  s.list().forEach((w, i) => s.update(w.id, { sql: String(i) }));
  s.move(0, 2);
  check('a window dragged to the end lands there',
    s.list().map((w) => w.sql).join('') === '120', s.list().map((w) => w.sql).join(''));
  s.move(2, 0);
  check('and back to the front', s.list().map((w) => w.sql).join('') === '012');
  s.move(0, 99);
  check('an out-of-range drop clamps rather than throwing',
    s.list().length === 3 && s.list().map((w) => w.sql).join('') === '120');
  check('moving a window onto itself changes nothing', s.move(1, 1).ok === true);
}

/* ── 7. Bulk closes ──────────────────────────────────────────────────────── */

{
  const s = store();
  s.init(); s.create(); s.create(); s.create();
  const keep = s.list()[1].id;
  s.closeOthers(keep);
  check('close others leaves exactly the one', s.list().length === 1 && s.activeId() === keep);
  check('and the others are reopenable, not gone', s.canReopen() === true);
}
{
  const s = store();
  s.init();
  s.update(s.activeId(), { sql: 'saved' }); s.markSaved(s.activeId());
  s.create(); s.update(s.activeId(), { sql: 'unsaved work' });
  s.create();                                     // empty, untouched
  s.closeUnmodified();
  const left = s.list();
  check('close unmodified keeps the one with unsaved work',
    left.length === 1 && left[0].sql === 'unsaved work',
    left.map((w) => w.name + ':' + w.sql).join(' | '));
}
{
  const s = store();
  s.init(); s.create();
  s.closeAll();
  check('close all still leaves one empty window to type into',
    s.list().length === 1 && s.list()[0].sql === '');
}

/* ── 8. Persistence ──────────────────────────────────────────────────────── */

{
  const disk = fakeStorage();
  const a = store(disk, 'projA');
  a.init();
  a.update(a.activeId(), { sql: 'SELECT * FROM one', conn: 'target' });
  a.rename(a.activeId(), 'Lookup');
  a.create();
  a.update(a.activeId(), { sql: 'SELECT * FROM two' });
  a.addHistory(a.activeId(), { sql: 'x', ms: 5 });
  a.update(a.activeId(), { results: { rows: [{ a: 1 }], cols: ['a'], ms: 5 } });
  const activeBefore = a.activeId();

  const b = store(disk, 'projA');            // a reload
  b.init();
  check('the windows come back after a reload',
    b.list().length === 2 && b.list()[0].sql === 'SELECT * FROM one'
    && b.list()[1].sql === 'SELECT * FROM two',
    b.list().map((w) => w.sql).join(' | '));
  check('with their names', b.list()[0].name === 'Lookup');
  check('their connection', b.list()[0].conn === 'target');
  check('and the same one active', b.activeId() === activeBefore);

  check('but NOT their result sets — a stale photograph of a database is worse than none',
    b.list()[1].results === null, JSON.stringify(b.list()[1].results));
  check('nor their history, for the same reason', b.list()[1].history.length === 0);
  check('and nothing running is restored as running', b.list().every((w) => w.running === false));

  const c = store(disk, 'projB');            // a different project
  c.init();
  check('another project gets its own windows, not this one\'s',
    c.list().length === 1 && c.list()[0].sql === '');

  const raw = disk.__map[W.KEY_PREFIX + 'projA'];
  check('the stored document holds no result rows at all',
    raw.indexOf('"rows"') === -1 && raw.indexOf('"results"') === -1, raw.slice(0, 200));
  check('and no connection string — only which side was chosen',
    /"conn":"(source|target)"/.test(raw) && !/Password|postgres:\/\//.test(raw));
}
{
  const disk = fakeStorage();
  disk.setItem(W.KEY_PREFIX + 'p', 'not json at all');
  const s = store(disk, 'p');
  s.init();
  check('a corrupt stored document starts a clean session rather than a blank page',
    s.list().length === 1 && s.activeId() === s.list()[0].id);
}
{
  const s = W.makeStore({ storage: null, projectId: 'p' });   // private mode
  s.init(); s.create();
  check('with no storage at all the session still works, it just does not persist',
    s.list().length === 2);
}

/* ── 9. Renaming ─────────────────────────────────────────────────────────── */

{
  const s = store();
  s.init();
  s.rename(s.activeId(), '  Customer   counts  ');
  check('a rename is trimmed and collapsed', s.active().name === 'Customer counts', s.active().name);
  check('an empty rename is refused, because a nameless tab is unclickable',
    s.rename(s.activeId(), '   ').ok === false && s.active().name === 'Customer counts');
  s.rename(s.activeId(), 'x'.repeat(200));
  check('an absurd name is capped rather than breaking the strip', s.active().name.length === 60);
}

/* ── 10. Cycling and jumping ─────────────────────────────────────────────── */

{
  const s = store();
  s.init(); s.create(); s.create();
  s.activate(s.list()[0].id);
  s.cycle(1);
  check('cycling forward moves one along', s.activeId() === s.list()[1].id);
  s.cycle(-1); s.cycle(-1);
  check('and wraps round the end rather than stopping', s.activeId() === s.list()[2].id);
  s.activateIndex(0);
  check('jumping to the nth window works', s.activeId() === s.list()[0].id);
  check('jumping past the end does nothing rather than blanking the editor',
    s.activateIndex(9).ok === false && s.activeId() === s.list()[0].id);
}

/* ── 11. Wiring ──────────────────────────────────────────────────────────── */

const html = read('public', 'sql-editor.html');
const app = read('public', 'sql-editor-app.js');

check('the page loads the store', /<script src="\/cygenix-sql-windows\.js/.test(html));
check('there is a tab strip above the toolbar',
  html.indexOf('id="sqlw-strip"') !== -1
  && html.indexOf('id="sqlw-strip"') < html.indexOf('class="editor-toolbar"'),
  'the strip belongs above the toolbar, not below it');
check('with a new-window button and a window menu',
  /id="sqlw-new"/.test(html) && /id="sqlw-menu-btn"/.test(html));
check('the menu is rendered in a portal on <body>, so the toolbar cannot clip it',
  /document\.body\.appendChild\(menu\)/.test(app) || /sqlwMenuPortal/.test(app));

check('one Monaco editor with a model per window, not one editor per window',
  /createModel\(/.test(app) && !/monaco\.editor\.create\(host\)[\s\S]{0,200}forEach/.test(app));
check('switching windows saves and restores the editor view state',
  /saveViewState\(\)/.test(app) && /restoreViewState\(/.test(app),
  'cursor, selection and scroll position come back only if this is done');
check('a query that is running keeps writing to the window that started it',
  /const winId = \(typeof sqlwStore/.test(app)
  && /function sqlwResult\(winId/.test(app)
  && /s\.activeId\(\) === winId/.test(app),
  'otherwise a background result paints over whatever tab you switched to');
check('and the Run button and the Results tab follow the screen, not the run',
  /const stillHere = /.test(app) && /if \(stillHere\) \{\s*\n\s*switchBottomTab/.test(app),
  'being yanked back to a finished query you had left is its own bug');
check('the assistant opens a new window instead of overwriting one',
  /A new window, not the current one/.test(app) && /sqlwNew\(\{ sql, name: 'AI query' \}\)/.test(app));
check('and falls back to the current window at the cap rather than losing the SQL',
  /losing the SQL entirely/.test(app));
check('the old single-key autosave is disarmed, not left fighting the store',
  /if \(window\.CygenixSqlWindows\) return;/.test(app) && /SUPERSEDED by the window store/.test(app));

/* The shortcuts are registered twice on purpose. Monaco takes the keyboard
   while it has focus and stops some Alt combinations from reaching the
   document at all — Alt+W did nothing with the cursor in the editor, which is
   where the cursor almost always is. Caught in the browser, pinned here. */
check('the window shortcuts are Monaco commands as well as document handlers',
  /function sqlwBindEditorKeys/.test(app)
  && /ed\.addCommand\(K\.Alt \| C\.KeyW/.test(app)
  && /ed\.addCommand\(K\.Alt \| C\.KeyT/.test(app)
  && /ed\.addCommand\(K\.Alt \| C\['Digit' \+ n\]/.test(app),
  'Monaco swallows Alt+W, so a document listener alone never sees it');
check('and the document handler stands down inside the editor, so neither fires twice',
  (app.match(/closest\('#monaco-host'\)\) return;/g) || []).length >= 2);

check('the saved-script association is recorded per window, not per page',
  /function sqlwNoteContext/.test(app) && /function sqlwRestoreContext/.test(app)
  && /if \(typeof sqlwNoteContext === 'function'\) sqlwNoteContext\(type, id, name\);/.test(app),
  'one tab can be a script, the next a draft, the third a job');
check('saving a window clears its unsaved mark',
  /s\.markSaved\(w\.id, script\)/.test(app));
check('and clearing the results clears the window\'s cached copy too',
  /update\(sqlwStore\(\)\.activeId\(\), \{ results: null, error: null \}\)/.test(app),
  'otherwise switching away and back brings back rows the user just cleared');
check('tabs do not shrink to slivers — they overflow into the menu instead',
  /flex:0 0 auto/.test(html) && /min-width:96px/.test(html),
  'twelve shrinkable tabs all "fit" and none are readable');

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
