# Delete `ANTHROPIC_API_KEY` from Azure and Netlify

**This is not code and only you can do it.** No file in this repository declares
the variable — it was set by hand in two cloud consoles, and it has to be
deleted the same way. The code change landed on 2026-08-28: nothing reads it
any more, and `tests/anthropic-billing.test.js` fails the build if a read comes
back.

Until it is deleted, the guarantee holds for the code that is deployed **now**.
It does not hold for a rollback: redeploy any commit from before 2026-08-28
while the variable is still set, and the old call sites find it and start
billing again. Deleting the variable is what makes the guarantee survive a
rollback.

---

## 1. Azure — Function App `cygenix-db-api`

**Portal → Function App `cygenix-db-api` → Settings → Environment variables →
App settings.**

Delete `ANTHROPIC_API_KEY`. Save; the app restarts.

While you are there, `ANTHROPIC_MODEL` and `ANTHROPIC_MODEL_FAST` should
**stay** — they name which model to call, not who pays for it, and the routes
still read them.

## 2. Netlify — site `cygenix`

**Site configuration → Environment variables.**

Delete `ANTHROPIC_API_KEY`. It was read by the two edge functions, which are
now blocked and make no Anthropic call at all.

`ANTHROPIC_MODEL` should stay for the same reason as above.

## 3. Revoke the key itself

Deleting the variable stops Cygenix using the key. It does not stop anything
else that has a copy of it. The key was live in two cloud consoles and was
being spent by an endpoint any signed-in user could reach, so treat it as
exposed:

**console.anthropic.com → Settings → API keys → revoke.**

If you still want it for your own use, issue a new one and do not put it in
either console.

---

## What breaks, and what does not

| | |
|---|---|
| **Unaffected** | Everything the browser calls directly — SQL Editor, Object Mapping, the Assistant panel, Insights' own AI panels. These have always used the operator's own key from Settings and never touched the server's. |
| **Needs a key in Settings** | Report narrative, Data Quality's relationship suggestions, Insights' subject classification, Agentive Migration. These now ask the browser for the user's key and refuse without it. Anyone who has already set a key in **Settings → General** sees no change. |
| **Off entirely** | Scheduled profile building, `/api/analyse`, `/api/coworker`. They answer `503 USER_KEY_REQUIRED`. See `docs/assistant-ui-tools.md` and the notes in `scheduled-runner.js` for what re-enabling profile building needs. |

## Verifying afterwards

`https://cygenix.co.uk/api/health` should return `{status, timestamp}` and
**no** `apiKeyConfigured` field — that field was removed because an
unauthenticated endpoint should not be reporting on the site's secrets, and
because the day it answered `true` would be the day this guarantee had broken.

Then, in the Anthropic console, watch usage for a day. It should show only what
you spend yourself.
