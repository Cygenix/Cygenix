# Data Analyser

A menu item under **Connect**, at `/data-analyser`. Drop a file in; it works out what the
file is, says how confident it is and why, profiles every column, flags what will break on
load, and emits a target schema. Everything runs in the visitor's browser — no upload, no
server, no API key. That is the claim the page makes, and `tests/data-analyser.test.js`
pins it against the code: the engine and widget contain no `fetch`, no `XMLHttpRequest`,
no beacon, no socket, and write no browser storage.

## Files

| File | What it is |
|---|---|
| `public/cygenix-analyser.js` | The engine. Zero dependencies, UMD, works in a browser or Node. Exposes `CygenixAnalyser`. |
| `public/cygenix-analyser-widget.js` | The UI. Injects its own styles scoped under `.cygx`, mounts into any element. Exposes `CygenixAnalyserWidget`. |
| `public/data-analyser.html` | The console page that hosts it. |
| `tests/data-analyser-engine.test.js` | The vendor's own 123-assertion suite, against the renamed engine. |
| `tests/data-analyser.test.js` | The wiring, the rename, and the "nothing leaves the page" claim. |
| `tests/browser/data-analyser.smoke.js` | The page in a real browser, with the CDN blocked. |

## Why it is not called `cygenix-transform.js`

It arrived under that name, defining a global called `CygenixTransform`. This repository
already has `public/cygenix-transform.js` — the module that defines what a column
**transform** does to a value inside the migration runner — and it defines the same
global. The existing one guards its assignment; the new one overwrote unconditionally.
Which module a page got would have depended on script order, and the Pipelines screen,
which loads the runner's module, would have been the first casualty.

So both files and both globals were renamed on the way in. Nothing else changed.
`tests/data-analyser.test.js` loads the runner's module, the engine and the widget into one
scope and asserts all three globals coexist and are the right objects.

## Taking an upstream update

The vendor's headers are preserved beneath ours. To take a new version:

1. Diff the new `cygenix-transform.js` against `public/cygenix-analyser.js` below our header.
   There are exactly two renames to re-apply: `root.CygenixTransform = factory()` and the
   comment line naming the file.
2. Same for the widget: the UMD line, the `require('./…')` path, the throw message, and the
   header title (`'Data Analyser'` in place of `'Data format recogniser'`).
3. Diff their `test/run.js` against `tests/data-analyser-engine.test.js`. Ours differs only
   in the require path, the DOM gating, and the output shape.
4. Run `node scripts/stamp-assets.js`, then `npm test`.

If the global comes back under the old name, the structural test fails on the first check.

## How the page mounts it

```html
<div id="analyser"></div>
<script src="/cygenix-analyser.js"></script>
<script src="/cygenix-analyser-widget.js"></script>
<script>
  CygenixAnalyserWidget.mount('#analyser', {
    theme: consoleTheme(),      // 'dark' | 'light', read from <html data-theme>
    target: 'sql', dialect: 'sqlserver', table: 'imported_data'
  });
</script>
```

The widget has its own light and dark surfaces. The page hands it whichever the console is
showing and watches `<html data-theme>` so a switch in Settings re-themes it without a reload.
The console imposes exactly three things, by id because the widget injects its styles after
the page's: its accent, its type, and a hover tint derived from the accent.

`mount()` returns `{ load(text, name), result, destroy() }`, exposed on the page as
`window.CygenixAnalyserPage` so the smoke test, the assistant, or a console user can drive it.

## Optional libraries

Both are progressive. Loaded from cdnjs — the one host the console already trusts, for MSAL
— pinned, and `defer`red. Without them the widget says what is missing rather than failing
quietly, so a blocked CDN degrades a feature and never the page.

- **SheetJS** `xlsx@0.18.5` — `.xlsx` / `.xls` workbooks. Detected by magic bytes regardless.
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
- XML and HTML parsing use the browser's `DOMParser`. In Node that needs `jsdom`, which is not
  a dependency here, so six of the vendor's assertions are reported as SKIP rather than PASS
  when it is absent.
- Practical ceiling is roughly a 50 MB file. Above that, profile server-side.
- Nested JSON and XML flatten to dotted paths; scalar arrays join with `; `.

## What this is not, yet

It analyses a file and stops. It does not create a connection, open a job, or push the
emitted DDL anywhere. The obvious next step — "make this a source" — hands the profiled
columns to Object Mapping via `CygenixAnalyser.map()`, which already does fuzzy source→target
matching. That is a product decision about where a file becomes a project, not a widget
change.
