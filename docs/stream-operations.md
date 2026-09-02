# Stream operations: stopping everything, and seeing what is stopped

Two features that answer the same question from opposite ends — *is anything
actually flowing?*

---

## 1. Global pause and resume

One switch in the Data Stream header, mirrored on Home. During an incident, a
cutover window or maintenance on the source, replication stops in one action.

### What it acts on

| Status | Global pause | Global resume |
| --- | --- | --- |
| Running, Lagging, Snapshotting, Failed | pauses | restores if it was live at pause time |
| Paused (by a person, before) | **left alone** | **left alone** |
| Draft | left alone | left alone |

The second row is the important one. **A stream somebody deliberately paused a
fortnight ago must not come back to life because a colleague pressed global
resume.** The prior status of everything the global pause stopped is recorded
in `state.globalPause`, which is persisted and synced — it survives a reload,
another browser and another operator.

### The dialog is the feature

Each store keeps records for a fixed window. While delivery is stopped the
backlog ages, and when the oldest record passes the window that backlog is
gone: a full resync, not a replay. `retentionDeadline()` computes that from
the age of the oldest pending record — **age, not volume**: eleven records and
eleven million expire at the same moment if they arrived together.

The dialog leads with the worst case, costs every stream from its own window,
names what it is leaving alone, and puts focus on Cancel. A global action
takes the **strictest guardrail in scope** — one stream set to "Confirm every
change" makes the whole action require the typed word `PAUSE`.

Resume lists what restarts, what does not, and separates streams that were
failing with a non-empty dead-letter queue behind an opt-in that defaults to
unchecked: they will fail again the moment they start and add to the pile.

### No optimistic UI

An operator who believes replication has stopped when it has not is the worst
outcome this can produce. Streams are applied one at a time with a real frame
between them; a row waiting its turn shows that rather than a status about to
change; **a write that does not persist counts as a failure**; and a partial
result lands the switch in mixed state with the failed streams named and a
Retry, never a success toast.

One audit entry per action, carrying actor, count, prior-state snapshot and an
optional reason — the operator did one thing.

---

## 2. Blocked: a stream that is not delivering cannot look like one that is

### The failure this exists for

```
Source → Cutover delta into target
Status      Failed
Throughput  260/min        ← reassuring
Last event  0s ago         ← reassuring
Actually    capture 260/min, DELIVER 0/min, 1,600 pending and climbing,
            every recent event dead, ~95 in the dead-letter queue
            "destination rejected: string truncation on stg_customers.name"
```

Two of the three numbers on that row were comforting and **both were true**.
It *was* reading 260 changes a minute and it *did* see an event a second ago.
Nothing was arriving at the destination, and the cause was three clicks away
in a tab nobody had a reason to open.

### Detection — from the series, not the status label

| Rule | Level |
| --- | --- |
| (a) capture > 0 and deliver == 0 for 3+ minutes | **blocked** |
| (c) consecutive failures past the configured retry count | **blocked** |
| (b) dead-letter depth grew in the last 5 minutes | degraded |
| (d) store depth rose monotonically for 10+ minutes | degraded |

Degraded is a real distinction: a stream delivering 900/min while
dead-lettering 5/min is losing data but is not stopped, and lumping it in with
a full stop makes the loud state mean nothing.

### The band

Pinned above the KPI tiles on both Data Stream and Home — below the numbers,
the numbers get read first, and the numbers are the misleading part. Written
as sentences in the order the questions get asked:

- **what stopped** — *"Source → Cutover delta into target has delivered nothing for 13 minutes."*
- **why** — the destination's own message, in monospace, selectable, with the
  failing table and column picked out of it. Never paraphrased: it is what
  goes in the ticket.
- **what's piling up** — *"1,613 changes are queued and 95 have been given up on."*
- **what it means** — derived from the write mode and what is actually stuck.
  A merge stream dropping deletes says the target is drifting; an append-mode
  webhook says the feed simply stops with no gap to notice.
- **the deadline** — *"72h retention — about 58 hours before this backlog
  expires and needs a full resync."* Amber under 12h, red under 4h.
- **actions** — dead letters, requeue, resync, and Copy diagnostics (one
  paste-ready block for a ticket).

One band at a time, with a count and a Next when more than one stream is
affected. Dismissable for the session, not for good.

### The row

`THROUGHPUT` became **Capture/min** and **Deliver/min**, because one figure
cannot stand for two hops that disagree, and a deliver figure of zero beside a
live capture is in the error colour. `LAST EVENT` became **Last delivered**
and answers that question — the capture timestamp is kept on the title
attribute rather than shown as if it were the same fact. Blocked rows get a
left rule and a one-line reason, and sort to the top on time-to-expiry rather
than on their status label.

### Flow view

When deliver is 0 the arrow is drawn **broken** with the error attached, not
solid with `0/min` written on it. A solid arrow still reads as a connection —
the picture wins the argument with the label.

### States built

healthy · one stream blocked · several blocked · degraded · blocked **and**
paused (the clock freezes and says *"paused while blocked"* — nobody expects a
paused stream to be delivering) · deadline already passed (the message becomes
"full resync required" and Requeue is disabled with the reason on it).

---

## Where the code is

| File | Holds |
| --- | --- |
| `public/cygenix-datastream.js` | `blockedState`, `blockedStreams`, `blockedConsequence`, `blockedDiagnostics`, `parseErrorTarget`, `retentionDeadline`, `globalPauseState`, `pauseAllPlan`/`commitGlobalPause`, `resumeAllPlan`/`commitGlobalResume` — pure, Node-requirable |
| `public/cygenix-datastream-ui.js` | `blockedBand`, used by both screens so they cannot describe one incident differently |
| `public/data_stream.html` | The switch, the dialogs, the transition, the row |
| `public/dashboard-app.js` | Home's band (read-only) and the replication tile |
| `tests/data-stream.test.js` | 329 checks, in `npm test` |
| `tests/browser/stream-cards.smoke.js` | 50 checks on the real page |

### One thing the prompts asked for that the repo cannot have

`POST /api/streams/pause-all` and a server-side snapshot. There is no streams
API: the module is a client-side engine whose state lives in browser storage
and syncs to Cosmos. An endpoint with no server-side stream state behind it
would be worse than none, so the actions are engine functions returning
per-stream results — the shape an HTTP handler would wrap if the engine ever
moves server-side.
