# Data Stream: what is real and what is scaffolding

**Prompt 5, the investigate-and-report-first step.** No plan, no code yet — the prompt
asked to be told honestly first, and the honest answer changes what the plan should be.

---

## The answer in one line

**None of it touches a real database, broker or file system. All five screens are a
deterministic simulator behind a production-quality UI.**

This is not a discovery — the module says so itself, in its own header at
`public/cygenix-datastream.js:45`:

> Real connectivity. Nothing in this file opens a socket, reads a transaction log or
> writes to a broker — v1 is the module's shape, its state machine and its arithmetic,
> running against seeded demo data. The places a real backend would attach are marked.

Verified rather than taken on trust: `public/cygenix-datastream.js`,
`cygenix-datastream-ui.js`, `cygenix-datastream-page.js` and `data_stream.html` contain
**zero** references to `fetch`, `CygenixDataApi` or `db-connect` between them. There is
no network code to review. `Math.random()` is never called either — every figure is
`hash(seed, tickNo, streamId, purpose)`, so two fresh loads agree exactly and a refresh
resumes from the persisted tick.

---

## Component by component

| Component | Where | Real? | What is actually there |
|---|---|---|---|
| **Capture** | `capabilityCheck()`, `:1754` | **Simulated** | Infers the engine from a regex on the connection's *label* (`/postgres/i.test(label)`) and reads `conn.cdcEnabled` — a flag on a demo object, not a query against `sys.databases`. It does not connect to anything. |
| **Stream Store** | `topicsOf()`, `STORE_PREFIX` | **Simulated, but persistent** | A `localStorage` buffer under `cygenix_datastream_v1::`, capped at 500 events and 720 chart points. Real persistence of fake records. |
| **Delivery** | `destKind` enum, delivery state machine | **Simulated** | `['database','broker','webhook','file','cygenix-target']` are declared and the state machine moves records through `pending → delivered / retrying / dead` correctly. Nothing is sent anywhere. |
| **Monitor** | `data_stream_monitor.html` | **Real logic, simulated inputs** | Lag arithmetic, blocked-state detection, dead-letter counts and stale-consumer detection are genuine, tested code. They are computing over generated numbers. |
| **Change events** | `data_stream_events.html` | **Simulated** | Events are synthesised per tick from the seed. |
| **Designer** | `data_stream_designer.html` | **Real** | Configuration authoring is genuine — the stream object it produces is a real, validated shape. It just has nothing to run against. |

---

## The part that is genuinely valuable, and should not be rebuilt

The simulation is not throwaway. What is already real and tested:

- **The state machine.** Seven statuses, four capture methods, five destination kinds,
  three schema-drift policies, four write modes — all as enums the UI asks rather than
  invents, so a status string cannot drift between screens.
- **The arithmetic.** Lag, throughput, retention deadlines, dead-letter accounting,
  consumer staleness. One engine, node-requirable, tested on its own —
  `tests/data-stream.test.js`.
- **The operational vocabulary.** Blocked-state surfacing, the global pause/resume with
  a preview of what it costs, reset-to-checkpoint, downstream-consumer impact. This is
  the hard part of an operations console and it exists.

So this is a module with a finished skin and no skeleton — which is the *better* half to
have finished, because the semantics are settled and the connectivity work has a fixed
target to hit.

---

## What that means for the plan the prompt asks for next

Two things I would want agreed before writing it:

**1. The gap is bigger than "add connectivity."** Log-based CDC on SQL Server means
reading `cdc.fn_cdc_get_all_changes_*`, tracking LSNs, holding a durable cursor and
surviving a restart without replaying or skipping. That is a server-side, long-running
process. Cygenix today has no long-running process anywhere: Netlify Functions have a
10-second wall (26s on background), and Azure Functions here are request-scoped. **A
real stream needs a host that does not exist yet** — Container Apps, a WebJob, or
Functions with Durable Entities. That decision comes before any capture code, and it is
the single largest item.

**2. Two of the five things the prompt lists are not "not finished", they are
unstarted.** Row-level reconciliation and cutover delta re-sync are both declared in
`cygenix-capabilities.js` as `status: 'planned'` (`:223`, `:251`). There is no partial
implementation to complete.

**What I would cut to get one real stream running end to end soonest:** rowversion
polling only, one source (SQL Server), one destination (a database target), at-least-once
delivery, no snapshot, no schema drift handling, no reconciliation. That is the shortest
path that proves the host, the cursor durability and the delivery contract — which are
the three things everything else depends on. Log-based CDC and Kafka are more valuable
and neither teaches you anything the first one does not.

**What has to be there before a client's production database is on the other end:**
permission preflight that actually queries the source the way Server Migration verifies
sysadmin; durable cursors with restart-without-loss proven under kill-testing;
backpressure; the dead-letter path end to end including requeue; and reconciliation,
because at-least-once delivery without a way to prove the destination matches is a
liability rather than a feature.

---

## One thing to fix regardless

`cygenix-capabilities.js` lists Data Stream as `status: 'preview'`, `minTier: 'business'`
— the £799 tier. The capability manifest is honest. Confirm the pricing page and sales
material carry the same word, because "preview" and "seeded demo data" are not the same
claim, and Business is a tier people pay for.
