# Browser tests

Four suites that need a real browser, and are therefore **not** in `npm test` —
the Netlify build has no Chromium and adding one would slow every deploy for a
check nobody reads until it breaks.

```bash
node tests/browser/page-reader.smoke.js     # 96 checks
node tests/browser/assistant-loop.smoke.js  # 23 checks
node tests/browser/sidebar-nav.smoke.js     # 15 checks
node tests/browser/sql-windows.smoke.js     # 76 checks
```

Each starts a local static server over `public/`, drives headless Chromium
through `playwright-core`, and exits non-zero on the first failure. Together
they take about a minute — most of it the SQL Editor, which waits on real
queries.

| Environment variable | Default |
| --- | --- |
| `CHROMIUM` | `/opt/pw-browsers/chromium-1194/chrome-linux/chrome` |
| `SMOKE_PORT` | `8396` / `8397` / `8399` / `8401` |
| `MONACO_VS` | a cached copy under the system temp directory |

## What each one is for

**`page-reader.smoke.js`** — the DOM half of `cygenix-page-reader.js`.
`tests/page-reader.test.js` pins the *rules* against pure functions and against
the source, which is the right way to pin a rule; this proves they hold against
an actual document. A `position:fixed` toolbar is not mistaken for a hidden
control. A recycled list row is caught. A password field is written under
confirmation and still never read back.

**`assistant-loop.smoke.js`** — the agent loop, end to end, with a scripted
model standing in for Claude. Everything between the model saying *click this*
and the button moving is wiring — `executeAll`, the confirmation parking, the
trail, the budget, the brakes — and wiring cannot be tested with a regular
expression.

The single most valuable assertion in either file is this one:

```
PASS  AND THE BUTTON HAS NOT BEEN PRESSED
```

It runs after the model asks to click **Run job** and the panel parks for
approval. A confirmation dialog that appears while the act has already happened
is not a confirmation, and that is exactly the sort of failure that passes a
source-level review.

**`sidebar-nav.smoke.js`** — the dashboard nav actually switches views. It
exists because of a bug no source-level test could have caught: the nav's
"am I on the dashboard" check was a regex written against `/dashboard.html`,
and when the addresses went extensionless it silently became always-false.
Fifteen menu items stopped working, in silence, with no console error —
because nothing had failed, something had merely not happened. The regex was
valid, the branch was reachable, the fallback existed. What was wrong was the
relationship between a string and a deployed URL, and only a browser sitting
on that URL can see it.

Its sharpest assertion is that a nav click switches the view **in place**. A
repaired fallback will also land you on the right view — by reloading the whole
application for every click. That is the bug made survivable, not fixed.

**`sql-windows.smoke.js`** — the SQL Editor's query windows.
`tests/sql-windows.test.js` proves the *list* is right: order, the cap, the
close-and-reopen stack, what reaches storage. None of that is what a person
feels. What they feel is switching tabs and finding the cursor where they left
it, an undo that still knows what they typed, and yesterday's rows **not**
reappearing under today's query. Those are properties of Monaco models and view
state, and a grep would pass whether or not any of them worked.

It found two defects on its first run. Monaco takes the keyboard while it has
focus and swallowed **Alt+W** before it reached the document, so the close
shortcut did nothing with the cursor in the editor — which is where the cursor
almost always is; the shortcuts are now Monaco commands as well. And the tabs
were shrinkable, so a dozen windows all "fitted" as unreadable slivers instead
of overflowing into the Windows menu.

It needs Monaco, which the page loads from a CDN. Rather than reach the network
mid-test or commit 13MB of editor, it fetches `monaco-editor` once with
`npm pack`, caches it in the system temp directory, and fulfils the CDN
requests from there. Set `MONACO_VS` to an existing `min/vs` directory to skip
the fetch entirely.

## Writing another one

Serve `public/` with `charset=utf-8` on the JavaScript — without it the browser
reads UTF-8 source as latin-1 and every em dash in a label arrives as mojibake.
Netlify sets it; a bare `http.createServer` does not, and the first version of
`assistant-loop.smoke.js` failed on exactly that.

Gated console pages need a signed-in browser seeded through `addInitScript`
before the first navigation (see the top of `page-reader.smoke.js`) — and the
guard there is on **sessionStorage**, because `addInitScript` runs on every
navigation and a localStorage guard makes the second page skip the seed.
