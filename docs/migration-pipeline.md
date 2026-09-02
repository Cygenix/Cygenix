# Migration pipeline

The card at the top of `/dashboard`: connect → analyse → map → generate →
validate → cutover, what is sitting at each stage, and one sentence saying
what today's problem is.

## Why

The Home screen reported state and told no story. Eight cards of equal weight,
each true, none of them saying which one mattered — so a person had to read all
eight and do the joining up themselves, every morning.

The Data Stream page had already solved this. Its Flow view puts
`capture → Stream Store → delivery` in a row with the live rate on each hop and
labels the one that cannot keep up `BOTTLENECK`, which is what makes sense of a
stream showing "Failed" and "260/min" at the same time. This is the same idea
applied to the migration lifecycle, and it deliberately uses the same
treatment — a node card, an arrow with a label on it, a bottleneck pill.

## Where the code is

| File | Holds |
| --- | --- |
| `public/cygenix-pipeline.js` | The derivation: stages, states, the bottleneck rule, the narrative, the confidence drivers. No DOM, no network, Node-requirable |
| `public/dashboard-app.js` (`renderMigrationPipeline`) | Model → markup, and nothing else |
| `public/dashboard.html` | The card's markup and CSS |
| `tests/pipeline.test.js` | 60 checks over fixtures, in `npm test` |
| `tests/browser/pipeline.smoke.js` | 31 checks on the real page, with the network cut off |

The derivation is where the judgement lives — what counts as analysed, which
stage is the blockage, what the sentence says — and that is exactly the part
that must be provable against fixtures rather than eyeballed.

## Stage states

| State | Means | Shown as |
| --- | --- | --- |
| `done` | complete for the current scope | `✓ done`, green |
| `active` | in progress, healthy | `active`, accent |
| `attention` | started, with a gap | `attention`, amber, left rule |
| `blocked` | cannot proceed without a person | `blocked`, red, left rule |
| `waiting` | legitimately not started yet | `not started`, muted |

Every state carries a word. With colour ignored — `html.a11y-hc`, a monochrome
print, a colour-blind reader — the row still reads correctly.

## One bottleneck, or none

1. The earliest `blocked` stage.
2. Otherwise the `attention` stage that has cleared the least of its total.
3. Otherwise **none** — and no label is drawn.

The value of the Flow view is that it points at one thing, so a healthy project
gets no bottleneck rather than an invented worst stage. Cutover is exempt from
(2) while nothing has completed: a migration that has not cut over yet is not a
gap to point at, only the calendar makes it one — otherwise the card would
spend the whole project pointing at the end of it.

## The narrative

Deterministic, on screen at first paint, no network. Cases in worst-first order:
no project, no connections, no jobs, past target end, a failed job, a counter
inversion (below), one side connected, analysis outstanding, data quality never
run, validation short, mapping short, generation short, all green.

A previously generated **AI rewrite replaces it** when one is cached. It is
never fetched on a render: that call goes to Anthropic on the user's own key
(Cygenix holds none — see `DEPLOY-remove-anthropic-key.md`), and spending
someone's money on a page load is not a thing to do quietly. The ↻ beside the
timestamp asks for a rewrite.

## The counters can disagree

Nothing in the stored shape forces the funnel to narrow. "Analysed" tests for a
mapped target column; "SQL generated" tests a status or a stored SQL string.
A job imported with its SQL already written counts for the second and not the
first, so **generate can legitimately exceed map** — which is why the live Demo
project showed `ANALYSED 2` beside `SQL GENERATED 4`.

A funnel that quietly widened would look like a rendering bug, so the model
detects the inversion, flags the stage that is *behind*, and can make it the
narrative: *"4 jobs reached generate with only 2 through map. Check those jobs
before trusting the SQL — the two counts should not be able to disagree."*

The four KPI tiles also count **every project**, while the panel below them
counts the active one. Both now say which population they count.

## Cutover Confidence

`pfConfidence` in `cygenix-preflight.js` is a **weighted average** over the
components that have data — not a hundred points with deductions, so
"100 − 9 − 6" would be a fiction however tidy it reads. The breakdown shows each
component's weighted contribution, which really does sum to the score, beside
what that component costs against a perfect one, and a `Fix →` link. One
scoring function, one source of truth: the UI reads its parts and re-derives
nothing.
