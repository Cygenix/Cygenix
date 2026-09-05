# Data Analyser

The **Data import** tab of Connections. Drop a file in; it works out what the file is, says
how confident it is and why, profiles every column, flags what will break on load, and emits
a target schema. Everything runs in the visitor's browser — no upload, no server, no API key.
That is the claim the tab makes, and `tests/data-analyser.test.js` pins it against the code:
the engine and widget contain no `fetch`, no `XMLHttpRequest`, no beacon, no socket, and
write no browser storage.

## Where it lives, and where it used to

It was a page of its own, `/data-analyser`, under Connect in the sidebar, for a short while.
It is now inside the dashboard's Connections view on the Data import tab — the tab keeps its
name and its place in the strip (Database connections, Data import, Restore database, Linked
servers, Azure Blob Source). The sidebar item, the assistant's page entry and the page itself
are gone; `/data-analyser` and `/data-analyser.html` redirect to
`/dashboard?goto=connections/import`, and the dashboard's deep-link reader now takes a view
and a tab (`goto=<view>/<tab>`, from the hash or the query string).

### It joined the importer; it did not replace it

The tab already imported files into a database, and the analyser does not do that. The two
are complementary and both stay:

| | Data Analyser | Import steps (2 · Review schema for import, 3 · Choose target) |
|---|---|---|
| Reads | a bounded slice of one file, in memory — the first 20,000 rows, from at most the first 50 MB | the whole file, streamed — CSV up to ~2M rows, Excel up to 200k |
| Produces | a profile: format, confidence, every column's type and issues, key candidates, DDL, exports | a table in the source or target database, with the types chosen in the review table |
| Writes anything? | never | rows, to the database you choose |
| Way out | **Use as a source in Object Mapping** (a job record carrying names and types, never rows) | **Import to database** |

One drop zone feeds both. The router in `dashboard-app.js` (`impHandleFiles`):

- **One file** → `impAnalyserProfile(file)` for the analyser, and `impHandleFile(file)` for the
  import's schema preview — the same review-and-target steps as before. A file the analyser
  reads but the importer cannot (JSON, XML, YAML, INI, a SQL dump) is profiled and gets a note,
  not an error; the analyser's Export tab can turn it into a CSV.
- **Several files** → the batch flow, unchanged: auto-import to the source database as
  `dbo.<filename>` (one table per sheet, `<file>_<sheet>`, for a workbook), `NVARCHAR(MAX)`
  fallback on a column that fails validation. The analyser is hidden while a batch is on
  screen, because it profiles one file and a batch is many; **Clear batch** brings it back.

The two readers are independent on purpose. Sharing one read would either hold a 1 GB CSV in
memory for the analyser's sake or deny the analyser a file the importer cannot take. A text
file above 50 MB is profiled from its first 50 MB and the note under the drop zone says so;
a workbook cannot be sliced (its structure is at the end), so a workbook above 50 MB is not
profiled and the note says that instead. The import path is unaffected either way.

The Azure Blob Source tab's per-file import goes through the same router, so a blob is
profiled as well as parsed.

## Files

| File | What it is |
|---|---|
| `public/cygenix-analyser.js` | The engine. Zero dependencies, UMD, works in a browser or Node. Exposes `CygenixAnalyser`. |
| `public/cygenix-analyser-widget.js` | The UI. Injects its own styles scoped under `.cygx`, mounts into any element. Exposes `CygenixAnalyserWidget`. |
| `public/cygenix-analyser-handoff.js` | Profile → Object Mapping job, and the runner's refusal. Loaded on the dashboard and on Object Mapping. |
| `public/dashboard.html` | The Data import tab hosts it (`#imp-analyser-wrap`, `#analyser`, `#da-handoff`) and imposes the console's accent and type by id. |
| `public/dashboard-app.js` | `impAnalyserInit` / `impAnalyserProfile` / `impAnalyserHandoff` / `impAnalyserReset`, the router, and the `goto=<view>/<tab>` reader. |
| `tests/data-analyser-engine.test.js` | The vendor's own 123-assertion suite, against the renamed engine. |
| `tests/data-analyser.test.js` | The wiring, the rename, the move into the tab, the surviving import behaviour, and the "nothing leaves the page" claim. |
| `tests/data-analyser-handoff.test.js` | The hand-off record carries names and types and not one cell value. |
| `tests/browser/data-analyser.smoke.js` | The tab in a real browser: the redirect, one file to both halves, several to the batch, the hand-off. |

## Why it is not called `cygenix-transform.js`

It arrived under that name, defining a global called `CygenixTransform`. This repository
already has `public/cygenix-transform.js` — the module that defines what a column
**transform** does to a value inside the migration runner — and it defines the same
global. The existing one guards its assignment; the new one overwrote unconditionally.
Which module a page got would have depended on script order, and the Pipelines screen,
which loads the runner's module, would have been the first casualty.

So both files and both globals were renamed on the way in.
`tests/data-analyser.test.js` loads the runner's module, the engine and the widget into one
scope and asserts all three globals coexist and are the right objects.

## Taking an upstream update

The vendor's headers are preserved beneath ours. To take a new version:

1. Diff the new `cygenix-transform.js` against `public/cygenix-analyser.js` below our header.
   There are exactly two renames to re-apply: `root.CygenixTransform = factory()` and the
   comment line naming the file.
2. Same for the widget: the UMD line, the `require('./…')` path, the throw message, and the
   header title (`'Data Analyser'` in place of `'Data format recogniser'`). One line of
   behaviour differs too: `load()` puts at most the first 200,000 characters of the input in
   the paste box (the parse still gets all of it), because a 50 MB string in a textarea is
   50 MB the browser lays out.
3. Diff their `test/run.js` against `tests/data-analyser-engine.test.js`. Ours differs only
   in the require path, the DOM gating, and the output shape.
4. Run `node scripts/stamp-assets.js`, then `npm test`.

If the global comes back under the old name, the structural test fails on the first check.

## How the tab mounts it

```js
// dashboard-app.js — on the first switchConnTab('import'), never at boot
impAnalyser = CygenixAnalyserWidget.mount(document.getElementById('analyser'), {
  theme: impAnalyserTheme(),   // 'dark' | 'light', read from <html data-theme>
  target: 'sql', dialect: 'sqlserver', table: 'imported_data',
  samples: false,              // the "Try:" chips are for a page with nothing else on it
  onResult: () => { /* reveal #da-handoff */ },
});
```

The widget has its own light and dark surfaces. The tab hands it whichever the console is
showing and watches `<html data-theme>` so a switch in Settings re-themes it without a reload.
The console imposes four things, by id because the widget injects its styles after the page's:
its accent, its type, a hover tint derived from the accent — and it hides the widget's own
drop zone. The tab's drop zone above is the one entry point, because it takes several files
and the widget's takes one. The paste box stays: identifying a pasted sample as you type is
the analyser's, and the importer has no equivalent.

`mount()` returns `{ load(text, name), result, destroy() }`. The tab exposes the same handle
it always did, `window.CygenixAnalyserPage` — `load`, `profile(file)`, `result`, `handoff()`,
`destroy()` — so the smoke test, the assistant, or a console user can drive it. **Start over**
on the import steps resets the analyser too: a profile of one file next to the import of
another misleads.

## Optional libraries

Both are progressive, loaded by the dashboard's idle-time injector alongside PapaParse and
SheetJS, from cdnjs — the one host the console already trusts, for MSAL — and pinned. Without
them the widget says what is missing rather than failing quietly, so a blocked CDN degrades a
feature and never the tab.

- **SheetJS** `xlsx@0.18.5` — `.xlsx` / `.xls` workbooks. Detected by magic bytes regardless.
  The import path uses the same copy.
- **js-yaml** `4.1.0` — anchors, block scalars, multi-document YAML. Without it a built-in
  subset handles ordinary key/value and list-of-maps documents and adds a `YAML_SUBSET`
  warning.

## What it reads and writes

**Reads** — CSV, TSV, semicolon- and pipe-delimited, SOH- and tilde-delimited, fixed-width,
JSON, NDJSON, XML, HTML tables, YAML, INI / `.properties`, SQL `INSERT` dumps, single-column
lists, and Excel with SheetJS. Parquet, Avro, SQLite and PDF are identified from their magic
bytes and honestly refused — a browser cannot read them.

**Writes** — SQL (`CREATE TABLE` and batched `INSERT`s for SQL Server / Azure SQL, PostgreSQL,
MySQL, SQLite), CSV, TSV, JSON, NDJSON, XML, YAML, Markdown and HTML tables.

## What the profiler flags

The things that quietly corrupt a migration, surfaced rather than smoothed over:
`AMBIGUOUS_DATE`, `MIXED_TYPES`, `INT_OVERFLOW`, `CURRENCY_SYMBOLS`, leading zeros kept as
`string`, `RAGGED_ROWS`, `NO_HEADER`, `MULTIPLE_SHEETS`, `MULTIPLE_TABLES`, `BAD_LINES`,
`ALL_NULL`, `LONG_TEXT`, `POSITIONAL_NAME`. Primary-key candidates are proposed only from
key-shaped types and only from ten or more rows — uniqueness across three rows means nothing,
and the DDL says so instead of guessing.

## Honest limits

- Types, lengths and key candidates are inferred **from the rows given**. A starting point for
  review, not a claim about the full extract; the generated DDL says so in a header comment.
  The import's own review table infers from the first 500 rows and is the one whose types
  the import creates.
- XML and HTML parsing use the browser's `DOMParser`. In Node that needs `jsdom`, which is not
  a dependency here, so six of the vendor's assertions are reported as SKIP rather than PASS
  when it is absent.
- Practical ceiling is roughly a 50 MB file; above that the profile is taken from the first
  50 MB (text) or not taken (a workbook). Import streams the whole file regardless.
- Nested JSON and XML flatten to dotted paths; scalar arrays join with `; `.

## Handing it to Object Mapping

Once there is a result, the tab offers **Use as a source in Object Mapping**. That writes a
job record, flushes it to the account, and opens `/object-mapping?edit=<id>`.

### What the record carries, and what it never carries

The tab says nothing leaves it, and a job record is synced to the account like any other
job. So the record carries the column **profile** — names, inferred SQL types, nullability,
key candidates, the issue *codes* the profiler raised — and **not one row**. No samples, no
min/max, no distinct lists. `tests/data-analyser-handoff.test.js` builds a job from a file
with recognisable cell values and asserts none of them appear in the serialised record; the
browser smoke captures the actual save request and asserts the same. The tab copy says so
at the point of hand-off.

Types travel as T-SQL strings (`INT`, `BIT`, `NVARCHAR(50)`, `DECIMAL(12,2)`) because that is
what Object Mapping reasons over — `isCharType()`, `parseTypeLen()`, the truncation warnings.
The engine's own finer category (`email`, `uuid`, `money`) rides alongside as `analyserType`.

### Why the flush before navigating is not optional

Object Mapping hydrates `cygenix_jobs` from the cloud on load, and that **overwrites** local.
A job that had only been written to this browser would be gone on arrival. So the tab
awaits `CygenixSync.saveNow()` first. If the flush fails, the cloud copy is not fetched
either (`ensureKey` returns false and keeps local), so the two paths agree; the tab says
so and goes anyway.

### How Object Mapping takes a source that has no connection

`cygenix-analyser-handoff.js` is loaded on both pages. `fileSourceToSrcTable(job)` produces
the same shape `connectSrc()` builds for a database table, and `installFileSource()` makes it
the only entry in `srcAllTables`. From there the **existing** restore path — find by value,
`selectTable('src')`, `restoreJobMapping` — runs unchanged. Three places had to learn a
source can be a file, and nothing else did:

- `connectSrc()` peeks synchronously at the `?edit=` job before fetching, and re-checks
  after the fetch, so a real source connection cannot overwrite the installed entry.
- `saveAsJob()` rebuilds the record from scratch and would have turned the job back into a
  database job pointing at a table called `file:orders.csv`; it now carries the file fields
  over.
- The evidence map samples live rows from both sides and refuses a file source, saying
  why. AI map and mapping by hand work as normal.

### What it will not do

The runner refuses to **run** a file job: the console holds the profile, not the rows, so
there is nothing to read from. The refusal names the file and points at Data import
(Connections → Data import), whose import steps put the rows in a database through the
existing path — the same tab the profile came from. Making those two meet — a mapped file
job that runs — is the next step, and it is a runner change, not a mapping one.
