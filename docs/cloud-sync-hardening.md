# Cloud persistence: what broke, what was wrong about the diagnosis, and what changed

## The incident

`CYGENIX_DATA_FN_KEY` was missing from the Netlify deployment for roughly
three weeks. `netlify/functions/data-proxy.js` refuses to run without it — a
proxy that cannot authenticate to the Function App must not degrade into an
unauthenticated one — so every call to `/api/data` returned 503.

The variable has since been set. The saving outage itself is over. This is the
second half: making the *next* configuration failure loud, contained and
non-destructive.

## The diagnosis that was handed over, and the part of it that was wrong

The handover said the sync layer read the resulting `null` as "Cosmos is
empty" and rendered an empty account over the top of local data.

That is not what `init()` did. It guarded with `if (cloud && typeof cloud ===
'object')`, so a null response skipped the apply block entirely and local data
survived. **A 503 never wiped anything through `init()`.**

The real failure was quieter, and in some ways worse:

- **Saves silently no-opped.** `save()` got `null` back, `r?.saved` was falsy,
  and there was no error branch. The keys stayed marked dirty — correct — but
  the only thing that would ever flush them again was the user happening to
  make another edit, and that flush hit the same 503. No retry, no queue, no
  surface. Every edit in between was kept nowhere but that browser.
- **Any second device showed an empty account**, because nothing came down and
  nothing was there locally.

This matters for whoever continues the work: hardening `applyCloud` against a
null response fixes a path that was already guarded. The save queue is the
part that addresses what actually happened.

## The data-loss path that WAS live, and was not in the handover

`init()` called `localStorage.removeItem()` for every `FIELD_MAP` key the cloud
response did not carry. The only guard was `Object.keys(cloud).length === 0` —
a *wholly* empty object. **A partial response deleted local data for every
field it omitted.**

That is not hypothetical. `FIELD_MAP` carries its own note on `conv_project`
and `last_snapshots`:

> If save succeeds but these don't round-trip back on load, the backend needs a
> matching schema update.

So the code deleted local state for exactly the fields somebody had already
flagged as possibly not coming back.

The fix is a rule, not a special case: **an omitted field is not a deletion.**
`save()` serialises whatever is in localStorage, so a user who deletes all
their jobs sends `jobs: []` — a present, empty value. Absence means the field
never made the round trip. A present value is applied; an absent one leaves
local alone.

## What changed

### One place where cloud state becomes local state

`applyCloud()` in `public/cygenix-cosmos-sync.js`. There used to be four such
places — `load()`, `forceLoad()`, `init()` and `resetToCloud()` — each with
slightly different clearing rules and only one with any guard. Two rules now
hold everywhere:

1. An omitted field is not a deletion (above).
2. Nothing empties everything at once. A verified response that would empty
   every key currently holding real data is a backend fault, not fifteen
   simultaneous deletions. Per-field emptying still propagates; all of it at
   once is refused, and the health state says `suspect-empty`.

Unverified responses cannot reach it at all.

### Failures are told apart

`public/cygenix-data-api.js` gained `callResult()`, which returns
`{ok:true, data}` or `{ok:false, error}` and never throws. `ok` says whether
the answer is trustworthy; `data` says what it was. `classify()` maps a status
and the proxy's own `code` onto a closed set: `config`, `auth`, `network`,
`server`, `client`, `no-token`.

`callOrNull()` is **deliberately unchanged and still fail-open.** Its three
call sites — the tier gate, the user pill, the billing portal link — must not
lock a paying user out when a billing lookup 500s. The handover's branch made
it throw; doing that here would trade a silent-data bug for a lockout. The
sync layer moved onto `callResult` instead, which is where the distinction was
actually needed.

### Failed saves are queued, counted and surfaced

Dirty keys survive a failure (they always did), and now a retry is scheduled
on a capped backoff (5s → 15s → 45s → 2m), the pending count is published, and
a `cygenix-sync-health` event fires on any change. `CygenixSync.getHealth()`
and `retryPending()` are the public surface.

The point of the backoff is not to out-wait an outage. It is that when someone
finally sets the missing variable, the queued edits go up on their own instead
of needing a lucky keystroke.

### The recovery helpers no longer destroy what they are recovering

- `resetToCloud()` **fetches first and wipes second.** It used to remove every
  sync key and *then* call the API, returning `{local_now_empty: true}` when
  the load failed — a recovery helper that destroyed the last copy of the data
  whenever the thing being recovered from was still broken. It also now
  refuses against a verified-empty cloud, since wiping local to match would
  leave no copy anywhere.
- `nuke()` **reads the cloud before overwriting it.** It used to push a
  near-empty document over the user's whole partition without ever looking at
  what was there.

### The failure has somewhere to appear

`public/cygenix-sync-banner.js`, on all 19 pages that load the sync module. It
says three things in this order: your work is not being saved; because
&lt;reason, in your terms&gt;; N changes are held on this device. Then
**Download my data** first and **Retry now** second — if this browser holds the
only copy, getting it onto disk matters more than another attempt.

It does not block the page. The data layer being down does not stop anyone
editing a mapping; it stops that work being *kept*, and the right response is
to say so clearly. (An earlier draft of this work gated every page load behind
a probe of `?action=ping`. On a healthy deployment that returns **401**, so it
would have declared a working site permanently degraded and blocked every
user.)

### The deployment cannot ship broken quietly

- `scripts/check-env.js` runs first in the Netlify build and **warns loudly** when
  `CYGENIX_DATA_FN_KEY` is absent — it no longer fails the build. Its first version
  did, and blocked every production deploy for two days when the variable was set in
  Netlify but scoped only to Functions, which the build cannot see. The runtime is
  loud enough on its own now (below); `CYGENIX_ENFORCE_ENV=true` opts back into the
  hard gate. `CYGENIX_DATA_API_BASE` is **not** required:
  `data-proxy.js` falls back to the production hostname and that literal is
  correct, so requiring it would have failed the very deploy carrying the fix.
  Presence is checked, never content — no value, no fragment, no length.
- `data-proxy.js` puts a machine-readable `code` on every error, logs the
  missing-key case at `console.error` (Netlify surfaces error-level function
  logs; a 503 with no log entry is how a fault stays invisible), and serves an
  unauthenticated `?action=health`.

#### On `?action=health` versus `?action=ping`

`ping` requires a token, so an unauthenticated probe of it returns 401 on a
*healthy* deployment. But note the ordering in `data-proxy.js`: the `FN_KEY`
check returns 503 **before** token verification. An unauthenticated probe is
therefore a perfect discriminator for this exact incident — **503 means the
missing variable, 401 means healthy.** The mistake in the original guard was
its classifier, not its probe. `health` makes that explicit rather than
implicit, and needs no probe token.

## Tests

- `tests/cloud-sync-resilience.test.js` — 70 checks. Runs the real modules in a
  sandbox against a scripted data layer, because every one of these bugs is
  about what the code *does* with a failure and a grep cannot see that.
- `tests/browser/sync-degraded.smoke.js` — 13 checks. Loads the real dashboard
  with `/.netlify/functions/data-proxy` stubbed to 503, asserts the banner
  appears and says the right things, that local data survived, that an edit
  made during the outage is queued — then flips the stub to healthy and
  asserts the queued work goes up and the banner clears.

## Still outstanding, and not code

1. **Rotate both Azure Function host keys** and set `REQUIRE_TOKEN_AUTH=true`.
   The old key is still in git history in `public/auth-gate.js` and five other
   files. Removing it from the client stopped new exposure; only rotation
   revokes what is already out. See `docs/DEPLOY-auth-fix.md`.
2. **A round-trip canary.** Nothing here proves data survives a full
   save-then-load against the real backend. Configuration is now checked at
   build time and reported at runtime; persistence itself is still unmonitored.
3. **Whether any job data ever reached Cosmos** from an earlier
   correctly-configured deploy is only answerable in the Azure portal. A load
   against a keyless proxy proved nothing came back, not that nothing is there.
4. **The auth fail-open question** is untouched, deliberately. No
   authentication behaviour was changed.
