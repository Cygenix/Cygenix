# Enriched stream cards

Three additions to `/data-stream`: a trend behind every KPI tile, a failure
panel that says *why* rather than only *where*, and the people downstream of a
stalled stream.

## 1. Sparklines, and the delta that carries the meaning

The four tiles moved on every refresh — errors 191 → 193 → 194 — and each
showed a bare instantaneous number, so the movement was invisible unless you
happened to be staring at it.

- **Delta label first.** `+2/min` for errors, `↑ 8%` for throughput. The
  sparkline shows the shape and is secondary to it.
- **Inline SVG polyline**, ~30 lines of arithmetic in `sparkPoints()`. No
  charting dependency was added, which is the correct trade for a 24px line.
- **Under eight points, no line at all** — the tile says `collecting…`. A flat
  line drawn from three samples is a lie told in a confident font.
- **Colour is for problems.** Red only when the series has moved ≥10% the wrong
  way given `higherIsBetter`; never green for "good and rising", or the colour
  stops meaning anything.
- **The accessible name is a sentence**: *"Delivery errors: 194, rising about 2
  per minute over the last minute."* The `<svg>` is `aria-hidden`. A list of
  numbers is not an alternative to a picture, it is a worse version of one.

**There is no metrics-history endpoint to ask.** The stream engine is a
client-side simulator with a two-second tick, so the history is a rolling
buffer (`state.kpiHistory`, 60 samples) written by `pushKpiSample()` on each
tick and persisted with the state. Every tile reads the same buffer, so two
tiles cannot disagree about what "the last minute" means.

## 2. Failure detail

`Failed`, `1h 1m` lag, `7,387 pending`, `224 in dead-letter` — every one of
those is *where*, and none of them is *why*.

The Errors tab now leads with the **error class** (`String truncation`,
`Constraint violation`, `Permission denied`…), then **how long it has been
failing** and since when — which the lag figure does not answer, because lag
says how far behind the data is, not when the trouble started — then the **last
successful delivery** as `14:02, 1h 4m ago`.

The hex checkpoint stays, because an engineer replaying by position needs it,
demoted to a `--code-tint` mono chip behind the copy button it already had.

**One dead-lettered record is shown with its field names and its values
redacted.** A status screen is read over shoulders and pasted into tickets;
customer rows do not go into one unless somebody deliberately asks. `Reveal
values` is a separate click and is not sticky — the next repaint redacts again.
Columns masked at capture stay masked either way.

Three actions, all through the page's existing guardrail (`confirmText`), not a
bespoke confirm: **Retry dead letters**, **Reset to last good checkpoint** (new
— re-delivers from the last position the stream is known to have delivered
from; the Stream Store still holds everything captured since, which is the
point of a checkpoint), and **Pause**.

## 3. Downstream consumers

`Source → Reporting warehouse` had been paused eight days. Somewhere a
reporting team was reading eight-day-old data, and this screen was the only
thing in the building that knew.

Streams carry `consumers: [{ name, owner?, notifyOnStall? }]`, edited in the
designer's Delivery step, one per line. Rendered on the row and in the Flow
card as `Consumers: Finance reporting, Client portal`; when the stream is
paused, failed, stopped or lagging, an amber impact line names them:

> Finance reporting has had no new data for 8 days.

Named consumers only — "consumers may be affected" is not information. A
stalled stream **with** consumers also lifts half a step in the attention sort:
above one nobody reads, never above an outright failure. Both are broken; only
one is quietly feeding stale numbers to a person.

`notifyOnStall` is stored and not yet acted on. The notifier is deliberately
out of scope; the data model is shaped so it is possible.

## The seeded clock

The demo seeds at a **fixed instant** so two seeds agree exactly — that is what
makes the figures reproducible and the tests meaningful. The live tick uses the
real clock, and the gap between them landed in every "how long ago" on the
screen: a stream seeded as failing twenty seconds ago read as *failing for 224h
31m* the moment the first live tick arrived.

`rebaseToNow()` shifts a seeded state onto the live clock once, when the page
starts ticking. Every timestamp moves by the same delta, so every interval
between them survives exactly as seeded.

## Where the code is

| File | Holds |
| --- | --- |
| `public/cygenix-datastream.js` | `trend`, `sparkPoints`, `kpiSeries`, `failureDetail`, `redactRow`, `consumerImpact`, `resetToCheckpoint`, `setConsumers`, `rebaseToNow` — all pure, all Node-requirable |
| `public/data_stream.html` | The tiles, the Errors panel, the consumer lines |
| `public/data_stream_designer.html` | The consumers editor |
| `public/cygenix-datastream.css` | Sparkline, impact line, dead-letter table |
| `tests/data-stream.test.js` | 242 checks, in `npm test` |
| `tests/browser/stream-cards.smoke.js` | 31 checks on the real page |
