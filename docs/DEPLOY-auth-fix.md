# Required actions after the auth-bypass fix

**Two of these are not code and only you can do them. Until both are done, the bypass is only
partly closed.**

---

## 1. Set the Netlify environment variable — before this deploy goes live

Netlify → Site configuration → Environment variables:

| Variable | Value |
|---|---|
| `CYGENIX_DATA_FN_KEY` | the Function App host key |
| `CYGENIX_DATA_API_BASE` | *(optional)* defaults to the current Function App `/api/data` URL |

**Set this to the NEW key from step 2, not the old one.** If it is missing, `data-proxy` returns 503
and says so — it will not fall back to an unauthenticated call. That is deliberate, but it means
sync, billing, the admin console and the agentive page stop working until the variable is set.

## 2. Rotate both Function App host keys — urgent

Azure Portal → Function App `cygenix-db-api` → App keys → rotate the default host key, and the
second key used by the agent routes.

Two keys were published in the client and committed to a GitHub repository:

```
WjSmoWxgtNdGnO_I5nKIspRUQqKCR1knsXgVmJr3dyYuAzFu-or-5Q==     (data, blob, admin, narrative surfaces)
UfQpm2qJuc0lWIbYwtGUm_k6ys_FDEquCma-KCPl_e8rAzFu5qfwbA==     (agent routes)
```

Removing them from the client stops **new** exposure. It revokes **nothing already copied**. Anyone
holding an old key can still call the Function App directly and spoof `x-user-id` until they are
rotated.

After rotating, put the new key into `CYGENIX_DATA_FN_KEY` (step 1) and redeploy Netlify.

## 3. Set `REQUIRE_TOKEN_AUTH=true` on the Function App — now safe, and now necessary

Azure Portal → Function App → Settings → Environment variables → `REQUIRE_TOKEN_AUTH` = `true`.

`azure-function/src/entra-auth.js` has always supported this. It could not be switched on because the
front end never attached a token, so enforcement would have 401-ed every call. The proxy now forwards
the caller's verified token, so this is safe.

**This is the control that actually stops header spoofing at the source.** With it off, the Function
App still falls back to trusting `x-user-id` for any request that arrives without a token — including
one from someone who kept an old key.

Watch the Function App logs for `[auth] no Bearer token on request` before flipping it. That warning
should have stopped appearing once this deploy is live.

## 4. Deploy the Azure Function

Unrelated to this fix but still outstanding from earlier work: `ANTHROPIC_MODEL` handling and the
`schema-view-deps` action both need a Function App deploy to take effect.

---

## What was changed in code

| Change | File |
|---|---|
| Authenticated proxy — verifies the Entra token, derives identity from verified claims | `netlify/functions/data-proxy.js` |
| Anonymous waitlist door, key held server-side | `netlify/functions/waitlist.js` |
| Client helper — token in, no key, no identity | `public/cygenix-data-api.js` |
| Keys and `x-user-id` removed from 11 client files | `auth-gate.js`, `cygenix-cosmos-sync.js`, `cygenix-api.js`, `cygenix-project-summary.js`, `reports-app.js`, `dashboard-app.js`, `admin.html`, `welcome.html`, `insights.html`, `data-quality.html`, `report.html`, `register.html`, `agentive_migration.html` |
| Behavioural test of the proxy | `tests/data-proxy.test.js` |
| Static guard against reintroduction | `tests/secret-exposure.test.js` |
| The suite now runs on every push | `.github/workflows/test.yml` |

## What is deliberately still outstanding

`BLOB_PROXY_CODE` in `dashboard-app.js`. The Drive's blob relay streams file **bodies**; routing them
through a Netlify function would cap every upload at 6 MB and break real workflows. It is also lower
severity — the relay carries the *customer's own* container SAS per request, so the key alone reaches
nobody's storage. The fix is Entra auth on the relay routes, which is an Azure-side change.

`tests/secret-exposure.test.js` asserts this is the **only** remaining site, so it cannot spread and
cannot be forgotten.
