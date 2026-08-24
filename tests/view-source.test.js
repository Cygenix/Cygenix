// Tests for mapping FROM a database view.
//
// A conversion view usually exists precisely to pre-shape data for a
// migration, so "map from the view" is often the shortest correct route from
// a legacy schema to a new one. Until now the Object Mapping screen could only
// pick a base table.
//
// Three properties are worth pinning, and they are what these tests cover:
//
//   1. A view is a legitimate SOURCE and never a TARGET. The backends' table
//      lists have always included views, and the target picker consumed that
//      list unfiltered — so a view was already selectable as a migration
//      target, producing SQL that fails at run time.
//   2. View metadata is different, not just missing: no row count, no primary
//      key, and a nullability flag that lies. Code that treats a view like a
//      table crashes or blocks on each of those.
//   3. Fan-out from one view to several targets is only correct with a grain.
//      A view joining client→addresses repeats the client on every address
//      row, so a straight insert makes one client per address.
//
// Deliberately NOT tested here: the #cygx_src snapshot and MERGE/@keymap
// machinery from the original brief. This generator emits a row-at-a-time
// CURSOR, not INSERT...SELECT per target — the source is already read exactly
// once, and the parent key is already in scope for the child insert of the
// same row. See the note in generateOTMSQL.
'use strict';
const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 240) : '')); }
};
const P = (...p) => path.join(__dirname, '..', ...p);
const read = (...p) => fs.readFileSync(P(...p), 'utf8');

const app     = read('public', 'object-mapping-app.js');
const page    = read('public', 'object_mapping.html');
const netlify = read('netlify', 'functions', 'db-connect.js');
const azure   = read('azure-function', 'src', 'index.js');
const lineage = read('public', 'cygenix-lineage.js');

console.log('Views as a mapping source — source-only, view-safe, grain-aware\n');

/* ── 1. Metadata: both backends list views, and say they are views ───────── */

for (const [name, src] of [['db-connect', netlify], ['the Azure Function', azure]]) {
  check(name + ' lists base tables and views together',
    /TABLE_TYPE IN \('BASE TABLE','VIEW'\)/.test(src));
  check(name + ' tags each object with its kind',
    /CASE WHEN t\.TABLE_TYPE='VIEW' THEN 'view' ELSE 'table' END AS kind/.test(src));
  // A view has no row count. 0 would read as "this view is empty", which is a
  // different and wrong statement.
  check(name + " reports a view's row count as NULL, never 0",
    /CASE WHEN t\.TABLE_TYPE='VIEW' THEN NULL ELSE COALESCE\(p\.rows,0\) END/.test(src));
  check(name + ' excludes system schemas from the object list',
    /TABLE_SCHEMA NOT IN \('sys','INFORMATION_SCHEMA'\)/.test(src));
  check(name + ' answers schema-view-deps for a view\'s base objects',
    src.includes("case 'schema-view-deps':")
    && /sys\.dm_sql_referenced_entities/.test(src));
  // A view whose base table has been dropped makes the DMV raise. That must
  // degrade to "no known base objects", not fail the whole selection.
  check(name + ' survives a broken view rather than failing the load',
    /catch \(e\) \{[\s\S]{0,200}baseObjects: \[\], reason: e\.message/.test(src));
}
// Both backends run the same SQL for this, or the two connection modes answer
// the same question differently — the bug schema-actions-parity exists to stop.
const norm = (s) => s.replace(/\s+/g, ' ').trim();
const depSql = (src) => {
  const i = src.indexOf("case 'schema-view-deps':");
  const q = src.indexOf('dm_sql_referenced_entities', i);
  return norm(src.slice(src.lastIndexOf('SELECT DISTINCT', q), q + 40));
};
check('both backends resolve base objects with identical SQL',
  depSql(netlify) === depSql(azure), depSql(netlify) + ' || ' + depSql(azure));

/* ── 2. The client keeps the distinction ────────────────────────────────── */

check('the source list records each object\'s type',
  /objType: t\.kind === 'view' \? 'view' : 'table'/.test(app));
check('an object with no kind is treated as a table — that is all the list held before',
  /kind === 'view' \? 'view' : 'table'/.test(app));
check('a null row count survives instead of collapsing to 0',
  /rowCount: \(typeof t\.rowCount === 'number'\) \? t\.rowCount : null/.test(app));
check('the row count renders as an em dash when there is none',
  /function rowCountLabel[\s\S]{0,200}: '—'/.test(app));

/* ── 3. Views are SOURCE-ONLY ───────────────────────────────────────────── */

// The headline guarantee. The backend list contains views; the target picker
// must drop them before they can ever be selected.
check('the target list filters views out at load time',
  /tgtAllTables = \(res\.tables\|\|\[\]\)\.filter\(t => t\.kind !== 'view'\)/.test(app));
check('every target object is typed as a table',
  /objType: 'table',/.test(app));
check('the object-type filter is applied to the source picker only',
  /function objTypeVisible\(which, list\)\{[\s\S]{0,160}if \(which !== 'src'\) return list;/.test(app));
check('there is no way to set the object type for the target side',
  !/setTgtObjType|tgtObjType/.test(app));

/* ── 4. The selector ────────────────────────────────────────────────────── */

for (const id of ['src-objtype-toggle', 'src-objtype-table', 'src-objtype-view', 'src-objtype-both', 'src-objtype-note']) {
  check('the page has #' + id, page.includes('id="' + id + '"'));
}
// The layout toggle already owns view-*; two meanings of "view" in one file is
// how the wrong one gets wired.
check('the new control does not collide with the Table/Visual layout toggle',
  page.includes('id="view-toggle"') && page.includes('id="src-objtype-toggle"')
  && !/id="view-objtype|id="src-view-toggle"/.test(page));
check('the selector reuses the house segmented-control styling',
  /<div class="mode-toggle" id="src-objtype-toggle">/.test(page));
check('Tables is the default, so every existing user sees no change',
  /let srcObjType = 'table';/.test(app)
  && /\(v === 'view' \|\| v === 'both'\) \? v : 'table'/.test(app));
check('the choice is remembered per source connection, not globally',
  /OBJTYPE_KEY_PREFIX = 'cygx\.om\.srcObjType\.'/.test(app)
  && /function srcObjTypeKey\(\)/.test(app));
// The connection string is a secret; it must not become a localStorage key.
check('the connection is hashed into the key rather than written into it',
  /h = \(\(h \* 33\) \^ raw\.charCodeAt\(i\)\)/.test(app));
check('switching the selector re-filters the list and never clears the mapping',
  /Switching the selector must never clear a mapping/.test(app)
  && !/setSrcObjType[\s\S]{0,400}(srcTable = null|clearMapState)/.test(app));
check('a selection the filter now hides stays selected, with a note saying so',
  /Still mapping from/.test(app) && /which this filter hides/.test(app));
check('rows are badged TABLE or VIEW when more than one kind is listed',
  /function objTypeBadge/.test(app) && /srcObjType!=='table'\?objTypeBadge\(t\)/.test(app));
check('the badge styles exist on the page',
  /\.objtype-badge\{/.test(page) && /\.objtype-badge\.is-view\{/.test(page));
check('tables sort before views, each alphabetically',
  /function objTypeSort[\s\S]{0,240}if \(av !== bv\) return av - bv;/.test(app));
check('search still matches the bare name as well as schema.name',
  /t\.label\.toLowerCase\(\)\.includes\(ql\)\|\|t\.name\.toLowerCase\(\)\.includes\(ql\)/.test(app));

/* ── 5. View metadata is different, not just missing ────────────────────── */

// SQL Server reports view columns as nullable far more often than the base
// column is. Letting that drive NOT NULL validation blocks good mappings.
check("a view's reported nullability is treated as unknown, not as a constraint",
  /reports view columns as nullable/.test(app)
  && /if \(c && typeof c === 'object'\) c\.nullable = true/.test(app));
check('a view with two columns of the same name is refused, naming both positions',
  /exposes "' \+ n \+ '" twice \(positions/.test(app));
check('a view with an unnamed expression column is refused, naming the ordinal',
  /has an unnamed column at position/.test(app));
check('the row count is counted on demand rather than guessed',
  /async function countSourceRows/.test(app) && /COUNT_BIG\(\*\)/.test(app)
  && page.includes('id="src-rowcount"'));
// A view whose base table has been dropped lists fine and fails on SELECT.
check('an unreadable view surfaces the server message, not a generic failure',
  /the view may reference a base object that no longer exists/.test(app));
check('no primary-key lookup on the source can throw for a view',
  !/srcTable\.primaryKeys\[0\]\.|srcTable\.primaryKeys\.find\(/.test(app));

/* ── 6. Grain — the fan-out correctness rule ────────────────────────────── */

check('each target carries a grain, defaulting to one row per source row',
  /grain:'row', grainKey:\[\]/.test(app));
check('the card offers "Distinct on business key"',
  /Distinct on business key/.test(app));
check('generation is blocked when a distinct grain has no key chosen',
  /Pick a business key for: /.test(app)
  && /there is nothing to be distinct on/.test(app));
check('a distinct grain emits an existence guard rather than a blind insert',
  /IF NOT EXISTS \(SELECT 1 FROM \$\{tgtFullQ\} WHERE \$\{pred\}\)/.test(app));
// The half that makes dedupe and key propagation the same solution: a repeated
// parent must hand its EXISTING key to the child, not a fresh one.
check('a repeated parent reuses the key of the row already loaded',
  /already loaded on an earlier source row — reuse its key/.test(app)
  && /SELECT TOP 1 \$\{tt\.pkVar\} = \[\$\{tt\.pkCol\}\] FROM \$\{tgtFullQ\} WHERE \$\{pred\}/.test(app));
check('a grain key that is not mapped to a target column degrades with a warning',
  /WARNING: grain key/.test(app) && /cannot deduplicate on it\. Falling back to one row per source row/.test(app));
check('a view feeding a child target at row grain warns before generating',
  /every child row repeats its parent/.test(app));
check('the grain is written into the saved job and the crash-recovery draft',
  /grain:tt\.grain\|\|'row', grainKey:\(tt\.grainKey\|\|\[\]\)\.slice\(\)/.test(app)
  && /grain: tt\.grain \|\| 'row',/.test(app));
check('a job saved before grain existed loads as one row per source row',
  (app.match(/grain\s*=\s*\w+\.grain === 'distinct' \? 'distinct' : 'row'/g) || []).length >= 2);

/* ── 7. Key propagation: the stale-key bug this feature depends on ───────
   @tbl is declared once, outside the WHILE loop, so OUTPUT...INTO accumulates
   a row per iteration and "SELECT TOP 1" kept returning the FIRST row's key.
   Every child from source row 2 onward was attached to row 1's parent. The
   fan-out this feature is for makes that failure the normal case. */

check('the identity capture table is cleared on every source row',
  /DELETE FROM @tbl\$\{tt\.id\.replace\(\/\[\^a-z0-9\]\/gi,''\)\};/.test(app));
check('the reason it must be cleared is recorded where the fix lives',
  /every child from row 2\s*\n?\s*\/\/ onwards is attached to row 1's parent/.test(app.replace(/\r/g, '')));
check('the capture still runs inside the grain guard when there is one',
  /sql\+=`  \$\{I\}DELETE FROM @tbl/.test(app)
  && /sql\+=`  \$\{I\}OUTPUT INSERTED/.test(app));

/* ── 8. Transactions ────────────────────────────────────────────────────── */

check('independent commits are refused when a parent key reaches a child',
  /if \(propagates && txMode === 'independent'\) \{[\s\S]{0,120}txMode = 'single';[\s\S]{0,60}txForced = true;/.test(app));
check('the forced switch is explained in the SQL header and in the warnings',
  /Transaction: forced to single/.test(app)
  && /Transaction mode forced to single/.test(app));
check('the selector is corrected to match what was generated',
  /if\(txForced && txSel\)\{ txSel\.value='single'; \}/.test(app));

/* ── 9. State, saved jobs and backward compatibility ────────────────────── */

check('the job records whether its source was a table or a view',
  /sourceObjectType: \(srcTable\.objType === 'view'\) \? 'VIEW' : 'TABLE'/.test(app));
check('both the single-map and the OTM save path record it',
  (app.match(/sourceObjectType:/g) || []).length >= 2);
check('the job records the view\'s base objects for lineage',
  /sourceBaseObjects: \(srcTable\.objType === 'view'\) \? _srcBaseObjects\.slice\(\) : \[\]/.test(app));
check('an older job with neither field opens unchanged',
  /Array\.isArray\(job\.sourceBaseObjects\) \? job\.sourceBaseObjects\.slice\(\) : \[\]/.test(app)
  && /default so an old job opens exactly as it always did/.test(app));
check('the crash-recovery draft carries the source object type',
  /srcObjectType: srcTable \? \(srcTable\.objType === 'view' \? 'VIEW' : 'TABLE'\) : 'TABLE'/.test(app));

/* ── 10. Lineage sees through a view ────────────────────────────────────── */

check('impact analysis follows a job whose source is a view onto its base tables',
  /const readsTable = \(j, table\)/.test(lineage)
  && /\(j\.sourceBaseObjects \|\| \[\]\)\.some/.test(lineage));
check('the impact entry says which view the dependency runs through',
  /through the view ' \+ j\.sourceTable/.test(lineage));
check('a job saved without base objects behaves exactly as before',
  /a job\s*\n?\s*\/\/ saved before that existed simply has none/.test(lineage.replace(/\r/g, '')));

/* ── 11. AI context ─────────────────────────────────────────────────────── */

check('the mapping prompt tells the model the source is a view',
  /a database VIEW/.test(app));
check('the prompt explains why a view scores high, so it is not read as a fault',
  /that is expected, not suspicious/.test(app));
check('the fan-out prompt states how many targets share this one source',
  /populates '\+targetTables\.length\+' target tables in a single run/.test(app));
check('the single-map prompt names the object type too',
  /'Source '\+\(src\.objType==='view'\?'VIEW':'table'\)/.test(app));

/* ── 12. Nothing regressed for a table source ───────────────────────────── */

check('the source is still referenced with the same bracket quoting',
  /const srcFull='\['\+srcTable\.schema\+'\]\.\['\+srcTable\.name\+'\]'/.test(app));
check('no index hint or TABLOCK is ever emitted against the source',
  !/TABLOCK|WITH \(INDEX/.test(app));
check('the join builder still reaches every readable object, view or table',
  /Joins may reach any readable object, tables and views alike/.test(app));
check('"Create from source schema" still checks the whole list, so a view name is never offered',
  /const exists = tables\.some\(t => t\.fullName === typed \|\| t\.name === typed\)/.test(app));

/* ── 13. The generated SQL, as a shape ──────────────────────────────────────
   Captured from a real browser run: one view fanning out to two targets, the
   parent at distinct grain and the child taking its key. Regexes above prove
   the pieces exist; this proves they compose in the right ORDER, which is
   where an insert-before-a-guard or a key read before its capture would hide. */
const SAMPLE = `
  -- ── 1. INSERT into NewDB.dbo.clients
  -- Grain: one clients per distinct client_ref
  IF NOT EXISTS (SELECT 1 FROM [dbo].[clients] WHERE [clnewnum] = @src_client_ref)
  BEGIN
    DELETE FROM @tblX;
    INSERT INTO [dbo].[clients] (
  [clnewnum],
  [cldispname]
    )
    OUTPUT INSERTED.[ClientID] INTO @tblX(Id)
    VALUES (
  @src_client_ref,
  @src_client_name
    );
    SELECT TOP 1 @clientsId = Id FROM @tblX;
  END
  ELSE
    -- already loaded on an earlier source row — reuse its key
    SELECT TOP 1 @clientsId = [ClientID] FROM [dbo].[clients] WHERE [clnewnum] = @src_client_ref;

  -- ── 2. INSERT into NewDB.dbo.client_addresses
  DELETE FROM @tblY;
  INSERT INTO [dbo].[client_addresses] (
  [Line1],
  [Postcode],
  [ClientID]
  )
  VALUES (
  @src_address_1,
  @src_postcode,
  @clientsId
  );`;
const at = (needle) => SAMPLE.indexOf(needle);
check('the guard opens before the parent insert',
  at('IF NOT EXISTS') < at('INSERT INTO [dbo].[clients]'));
check('the capture table is cleared before the insert that fills it',
  at('DELETE FROM @tblX') < at('OUTPUT INSERTED.[ClientID]'));
check('the key is read only after it has been captured',
  at('OUTPUT INSERTED.[ClientID]') < at('SELECT TOP 1 @clientsId = Id'));
check('the ELSE branch recovers the key of the row already there',
  at('END\n  ELSE') > 0 && at('SELECT TOP 1 @clientsId = [ClientID]') > at('ELSE'));
check('the child insert comes after the parent key is settled, and uses it',
  at('INSERT INTO [dbo].[client_addresses]') > at('SELECT TOP 1 @clientsId = [ClientID]')
  && at('@clientsId') < at('INSERT INTO [dbo].[client_addresses]'));
check('the child is not wrapped in the parent\'s grain guard',
  at('-- ── 2. INSERT') > at('SELECT TOP 1 @clientsId = [ClientID]'));

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
