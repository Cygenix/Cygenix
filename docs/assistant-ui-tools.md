# Generic UI tools for the assistant

The assistant acts through **typed actions**: `sql_run`, `stream_draft`,
`app_navigate`. Each is a named tool with a schema, a declared effect
(read / write / destructive) and a handler, which is what makes a run
auditable and what stops a screen redesign silently changing behaviour.

That is the right shape for the things worth naming, and the wrong shape for
the long tail. Cygenix has around fifty screens; only a fraction of their
controls will ever earn an action, and until now the assistant could not see
the rest — even though it is sitting on the page they are drawn on.

These generic tools are the other half: look at the screen, and operate it the
way a person would.

| Tool | Status |
| --- | --- |
| `read_page()` | **shipped** |
| `click(element_id)` | not built |
| `type(element_id, text)` | not built |
| `wait_for_change(description, timeout_ms)` | not built |

Reading shipped on its own on purpose. A tool that describes the screen can be
inspected, tested and trusted before anything is wired to move, and the
prompt, the description and the tests all say plainly that the assistant
cannot press anything — so it does not tell a user it has.

---

## `read_page()`

Client-side, in the page the user is looking at. `public/cygenix-page-reader.js`
does the work; `public/cygenix-assistant-actions.js` registers it as an action
with effect `read` and the trail mark `◉ Reading page`.

```json
{ "name": "read_page", "input_schema": { "type": "object", "properties": {} } }
```

No arguments: it describes the screen as it is. It returns

```js
{
  page_title: "Object Mapping",
  current_route: "/object-mapping",
  landmarks: [{ heading: "Source objects", element_id: "el_1a2b3c4d" }],
  interactive_elements: [
    { id: "el_5e6f7a8b", kind: "button", label: "Add mapping", section: "Source objects" },
    { id: "el_9c0d1e2f", kind: "input",  label: "Filter tables", value: "cust",
      section: "Source objects" },
    { id: "el_3a4b5c6d", kind: "button", label: "Save", disabled: true }
  ]
}
```

`kind` is one of `button`, `link`, `input`, `select`, `textarea`. A `role`
beats the tag it is written on, so a `<div role="button">` is a button.

### The element id

`el_` + the first 8 hex digits of
`sha256(accessible name, tag, type, nearest heading)`, all normalised for case
and whitespace. Two things are deliberately **not** in that hash:

- **The DOM `id` attribute.** Most controls in this console have none, and the
  ones that do are inconsistent about it. A scheme that works for a tenth of
  the page is not a scheme.
- **Screen position.** A coordinate changes when the user scrolls, when the
  window is resized, when a row above expands. An id that moves points at the
  wrong button a second later.

What is left is what a person uses to find a control again — what it says,
what kind of thing it is, what part of the page it sits in — and those survive
a re-render, a sort, a scroll and a filter. Where they genuinely collide (two
`Edit` buttons under one heading) the duplicates take a `_2`, `_3` suffix in
document order, which is stable for the same reason.

### What it refuses to look at

- Anything hidden: `display:none`, `visibility:hidden`, `opacity:0`,
  `[hidden]`, `aria-hidden`, `input[type=hidden]`, no client rects.
  `offsetParent === null` alone is not the test — it is also null for
  `position:fixed`, which is how every toolbar and modal here is anchored.
- The assistant's own panel. Listing its Send button is how an agent ends up
  talking to itself.

A **disabled** control is listed and flagged rather than dropped, so the model
knows the button exists and why it is not working.

### Secrets

Everything read_page returns is sent to the model, and a password box, a
connection string and a function key are all just `<input>` to the DOM. So the
refusal happens in the reader:

- `type="password"` — never read.
- A field whose name or label contains a credential word (`password`, `key`,
  `token`, `secret`, `conn`, `sas`, `bearer`, `auth`, …). The name is split
  into words on camel humps, underscores and hyphens first, so `fnKey`,
  `api_key`, `connString` and `Function key` are all caught and `tableName` is
  not.
- A **value** shaped like a credential — a connection string, a JWT, an
  `sk-` key — whatever the field is called.

The field is still listed in every case. The assistant needs to know the box
is there; it just does not get to read what is in it.

### Size

The whole snapshot is capped at 6 KB. A page here can carry thousands of
nodes, and a snapshot that does not fit is worse than a short one — it
displaces the conversation it was meant to inform. Elements are dropped from
the end, so the top of the screen always survives, and the number dropped is
reported rather than silently lost. A real console page (the SQL Editor)
measures about 35 elements and 2 KB.

---

## The brakes

Two stall detectors live in the runtime (`public/cygenix-assistant.js`), not
in the reader. Neither is a safety control — the guardrail policy is that —
they stop a run that has stopped making progress from spending the operator's
API budget circling.

1. **Tool-call budget.** Fifteen tool calls per user turn. The sixteenth ends
   the run with *"Task exceeded 15 tool calls — pausing for user input."*
   Asking again is what buys the next fifteen.
2. **Loop detection.** The same tool with the same arguments twice in a row
   ends the run with *"Detected a loop — asking user for guidance."*

When either trips, every outstanding tool call is answered with the reason and
the run stops — the results are **not** sent back for another turn, because
another turn is the thing being prevented. They are still written into the
conversation, so whatever the user says next continues from a valid transcript
rather than an assistant message with unanswered tool calls.

---

## What the next step needs

`click`, `type` and `wait_for_change` were specified alongside the reader and
are not built. When they are, these are the pieces already in place for them:

- `CygenixPageReader.lastRead()` records what the most recent read showed —
  when, on which route, which ids, and the nodes behind them. `click` is meant
  to refuse an id that was not in the last read, or a read older than 60
  seconds, and tell the model to read again.
- The confirmation dialog and the effect ladder already exist: an action
  registered `write` or `destructive` pauses under the project's guardrail
  policy without any new machinery.
- The third safeguard from the specification — the irreversible-action list
  (`Delete`, `Remove`, `Drop`, `Destroy`, `Confirm`, `Send`, `Submit`,
  `Publish`, `Pay`, `Purchase`, `Archive`) that forces a confirmation whatever
  else is true — belongs with `click`, since it is a rule about what may be
  pressed. It is not implemented yet, because nothing can press anything yet.
