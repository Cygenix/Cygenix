# SQL windows

The SQL Editor holds several queries at once, the way a browser holds several
pages. Each one is a **window**: a tab in the strip above the toolbar, with its
own SQL, its own results, its own history.

## Why

The editor was one query at a time. Comparing two result sets, or keeping a
lookup query beside the one being written, meant retyping — so people kept the
other query in a text file. That is the tell that the tool was missing
something: the workaround was outside the product.

## What belongs to a window

| Kept per window | Notes |
| --- | --- |
| SQL text | The live copy is a Monaco model; the copy that survives a reload is in the store |
| Cursor, selection, scroll | Monaco view state, saved on the way out and restored on the way in |
| Undo and redo history | A model owns its own undo stack, which is why each window has a model |
| Connection (source / target) | Two windows can be pointed at two different databases |
| Last result set, row count, timing, error | Cached — switching tabs never re-runs a query |
| That window's own History entries | |
| Unsaved-changes mark | The text differs from what was last saved or loaded |
| Saved script, job or Drive file | So Save means the right thing for whichever window is in front |

## What is deliberately not kept

**Results are never persisted.** A result is a photograph of a database at a
moment. Restoring one after a reload would show somebody yesterday's rows under
today's query with nothing saying so. Reloading returns each window to its
"Run a query" state, which is honest about what is known.

**No connection string reaches storage** — only which side was chosen.

## Keyboard

Alt-based, because the Ctrl/Cmd equivalents belong to the browser: a page
cannot take Ctrl+T, Ctrl+W, Ctrl+Shift+T, Ctrl+1–9 or Ctrl+Tab, and a shortcut
that works everywhere except where the user is looking is worse than none.

| Key | Does |
| --- | --- |
| `Alt+T` | New window |
| `Alt+W` | Close this window |
| `Alt+Shift+T` | Reopen the last closed window, where it was |
| `Alt+1` … `Alt+8` | Go to that window |
| `Alt+9` | Go to the last window, however many there are |
| `Ctrl+Alt+←` / `Ctrl+Alt+→` | Previous / next window |

Each is registered **twice**: as a document handler and as a Monaco command.
Monaco takes the keyboard while it has focus and swallows some combinations
before they reach the document — Alt+W never arrived — so the editor needs its
own binding, and the document handler stands down inside the editor so neither
fires twice.

Mouse: double-click a tab to rename it, middle-click to close it, drag to
reorder. The **Windows** menu lists everything open, says how many tabs do not
fit, and offers close-others / close-saved-and-unmodified / close-all.

## Where the code is

| File | Holds |
| --- | --- |
| `public/cygenix-sql-windows.js` | The list, as data: order, active, the cap, the closed stack, persistence. No DOM, no editor |
| `public/sql-editor-app.js` (`sqlw*`) | The controller: models, the strip, the menu, the keyboard, painting the page from the active window |
| `public/sql-editor.html` | The strip markup and its CSS |
| `tests/sql-windows.test.js` | The list's rules, without a browser |
| `tests/browser/sql-windows.smoke.js` | Everything a person actually feels, in headless Chromium |

The split is the point. The ordering, the reopen stack, the cap and what
reaches storage are the parts with real edge cases, and they are testable
without a browser only if they are kept away from one.

## Limits

Twenty windows. Not a technical limit — the point past which a tab strip stops
being navigable and the menu is doing all the work anyway. The twenty-first
says so rather than silently opening.

Ten closed windows are remembered for reopening.
