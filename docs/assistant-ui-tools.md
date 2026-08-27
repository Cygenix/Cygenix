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
| `click(element_id)` | **shipped** |
| `type(element_id, text)` | **shipped** |
| `wait_for_change(description, timeout_ms)` | not built |

Reading shipped on its own first, on purpose: a tool that describes the screen
can be inspected and trusted before anything is wired to move. `click` and
`type` then operate one element at a time, under the rules below. Waiting is
still not built.

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

## `click(element_id)`

```json
{
  "name": "click",
  "input_schema": {
    "type": "object",
    "properties": {
      "element_id": { "type": "string" },
      "reason": { "type": "string" }
    },
    "required": ["element_id"]
  }
}
```

Registered with effect `write` and the trail row `◉ Clicked: <label>`. The
rules live in `cygenix-page-reader.js`, next to the read they depend on.

### Four fences, and they are independent

**1. It must have looked.** An id that was not in the most recent `read_page`,
a read older than **60 seconds**, or a read taken on a different route is
refused outright. Every refusal names what to do next, because a refusal the
model cannot act on is a dead end:

- `There is no read_page result to work from. Call read_page first.`
- `The last read_page is 74 seconds old, which is over the 60-second limit. Call read_page again.`
- `"el_1a2b3c4d" was not in the last read_page result. Call read_page again and use an id from it.`
- `The screen has changed since that read_page (it was taken on /projects). Call read_page again.`

**2. It must still be the same element.** Node identity is not enough: a
virtualised list recycles its rows, so the node behind *Delete row 1* can
quietly become *Delete row 500*. The live label and section are re-derived and
compared with what was read; a mismatch, a removal, or the element becoming
invisible all give the specified message —
`Element el_1a2b3c4d no longer exists; call read_page again`.

**3. Irreversible things always ask.** A label containing **delete, remove,
drop, destroy, confirm, send, submit, publish, pay, purchase** or **archive**
confirms every time, whatever else is true. The match is a plain
case-insensitive substring, so it over-fires on the odd innocent label
(`Dropdown`) — which is the correct direction to be wrong in.

**4. Everything else asks too**, unless it is demonstrably harmless *and* was
read within **30 seconds**. The safe set is short and hardcoded:

- fields — `input`, `textarea`, `select` (clicking one only focuses it);
- links to another screen **within this app** (same origin, no `target`);
- search and filter buttons (`Search`, `Find…`, `Filter`, `Refresh`, `Sort…`,
  `Clear filters`).

`Cancel` and `Close` are deliberately **outside** it: cancelling a half-filled
form throws the operator's work away, and a control that only *might* be
harmless belongs on the asking side of the line.

A disabled control (`disabled` or `aria-disabled`) is refused rather than
clicked at, with a message telling the model to explain what it is waiting for.

### How the confirmation reaches the user

The panel's existing approval flow is reused rather than a second dialog being
invented — it already persists across a navigation and already writes the
audit row. Two additions make it fit:

- An action may answer the confirmation question **for itself** via a
  `confirms(input)` hook, because the consequence of a click is a property of
  the thing clicked, not of the action. Following a link and pressing *Delete*
  are the same action and not remotely the same act. The hook is consulted,
  not obeyed: it can *raise* a read to a confirmation, and it can clear one
  only where the action is not destructive. **The guardrail policy is still
  the floor.** Anything the hook cannot resolve confirms.
- An action may ask the question in its own words via `confirmTitle(input)`.
  `click` asks *Assistant wants to click "Save mapping". Proceed?* and the body
  gives the model's stated reason and why the confirmation was required. The
  buttons stay the panel's existing **Approve & run** / **Skip**.

### Navigation

An in-app link is **not** followed in the handler. It is handed back to the
runtime as `{__navigate: url}`, exactly as `app_navigate` does, so the run is
persisted and resumed on the next page rather than being dropped by the
browser.

### When the page goes anyway

A click can still navigate the browser itself — a form submit, a handler that
sets `location`. What survives is an assistant message whose tool calls were
never answered, and the API rejects the *next* turn outright when it sees one,
which kills the conversation for good. `healTranscript()` answers any such
orphan on the next load with *"The page reloaded before this finished, so the
result is unknown. Do not assume it ran."* A run parked on a confirmation or
waiting to resume is left alone — those get answered when they finish.

---

## `type(element_id, text)`

```json
{
  "name": "type",
  "input_schema": {
    "type": "object",
    "properties": {
      "element_id": { "type": "string" },
      "text": { "type": "string" },
      "reason": { "type": "string" }
    },
    "required": ["element_id", "text"]
  }
}
```

Effect `write`, trail row `◉ Typed into: <label>`. It inherits **fences 1 and
2 from `click`** unchanged — it must have looked, and the field must still be
the field it looked at — through the same `resolve()`.

### What it will fill in

A text `<input>`, a `<textarea>`, or a `contenteditable` region. Everything
else is refused and pointed at `click`: a checkbox, a radio, a range, a colour
picker, a `<select>`, a button. **A file picker especially** — its `value`
cannot be set from script by design, and a browser that allowed it would be a
browser with a hole in it. Read-only and disabled fields are refused too.

It **replaces** rather than appends: the field is cleared first, as a person
would. The write goes through the prototype's own `value` setter (what React's
test utilities do) so a framework value-tracker cannot swallow it, then
`input` and `change` are dispatched so listeners fire.

### The one confirmation

Ordinary typing goes straight in — nothing typed into a form reaches a system
until something is pressed, and that press has its own confirmation. Two
exceptions ask first:

- **A credential field** — decided by the *same* `isSensitive()` rule that
  makes `read_page` refuse to read those fields. One rule, so a field cannot
  be unreadable and freely writable at the same time. The dialog reads
  *Assistant wants to type into "Password". Proceed?* and shows the text,
  because the point of asking is that the operator can judge it.
- **A label on the irreversible list** — a *"Type DELETE to confirm"* box
  behaves the same way its button would.

The tool description and the system prompt both tell the model, in the
strongest terms available, never to invent a credential and type it in: it
does not know the user's secrets and must not guess at them. The confirmation
is the backstop, not the control.

### It never echoes the text

The result reports `typed_into` and a character count, and nothing else. The
model supplied the text, so the conversation already has it; repeating it back
gains nothing and, in a credential field, loses something. A value cap of
10,000 characters refuses a paste that has clearly gone wrong.

> **Considered and rejected:** refusing credential fields outright, the way
> destructive work is refused. Confirmation was chosen because a connection
> form is exactly where an operator might reasonably want help, and the
> refusal would land on the honest case as hard as the bad one.

---

## The brakes

Two stall detectors live in the runtime (`public/cygenix-assistant.js`), not
in the reader. Neither is a safety control — the guardrail policy is that —
they stop a run that has stopped making progress from spending the operator's
API budget circling.

1. **Tool-call budget.** Fifteen tool calls per user turn. The sixteenth ends
   the run with *"Task exceeded 15 tool calls — pausing for user input."*
   Asking again is what buys the next fifteen.
2. **Loop detection.** Two rules, both ending the run with *"Detected a loop —
   asking user for guidance."*
   - The same tool with the same arguments twice in a row.
   - The same element clicked twice with no `read_page` between them, even
     where other calls separated them. An action declares this with
     `repeatGuard(input)`; `read_page` clears it with `clearsRepeatGuard`,
     because looking at the screen is what makes a second press something
     other than pressing and hoping.

When either trips, every outstanding tool call is answered with the reason and
the run stops — the results are **not** sent back for another turn, because
another turn is the thing being prevented. They are still written into the
conversation, so whatever the user says next continues from a valid transcript
rather than an assistant message with unanswered tool calls.

---

## What the next step needs

`wait_for_change(description, timeout_ms)` is the last of the four and needs
no new plumbing: a `MutationObserver` on `document.body`, resolving on a new
landmark heading, a previously visible element disappearing, a URL change, or
text loosely matching the description; rejecting on timeout. It touches
nothing, so it can be a `read` and needs no confirmation.

Two things to decide rather than inherit:

- The trail rows are pushed **before** the handler runs, so `◉ Clicked: X`
  appears a moment before the click actually lands. That is the wording the
  specification asked for and it is fine at human speed, but a
  `wait_for_change` row has to update *in place* as it goes —
  `◉ Waiting for: …` then `◉ Detected: …` or `◉ Timeout` — which the trail
  cannot currently do.
- A timeout is not an error. If the page did not change, that is a fact worth
  reporting calmly; returning it as a failed tool call would push the model
  towards retrying rather than towards telling the user nothing happened.
