# Cygenix — architecture as built

**Produced:** 25 August 2026 · **Method:** read from the repository, not from the site
**Purpose:** the reference the Product Engineering Brief (v1.0) asks for in §0 before any work item starts.

The brief was written from outside the codebase and marks its guesses `ASSUMPTION:`. It asks that
each be verified and the document corrected where it is wrong. §7 below does that, item by item.

Read §1 first. It is not a work item.

---

## 1. The finding that outranks the brief

**The Azure Function App derived the caller's identity from a request header, and its host key was
published.**

```js
// azure-function/src/index.js
function getUserId(req) {
  return req.headers.get('x-user-id') || req.query.get('userId') || null;
}
```

That value became the Cosmos DB partition key. The only gate in front of it was
`authLevel: 'function'` — a host key that was hard-coded into **seven** client files and committed to
this repository:

| File | Constant |
|---|---|
| `public/auth-gate.js` | `FUNC_CODE` |
| `public/cygenix-cosmos-sync.js` | `FUNC_CODE` (re-exported as `CygenixSync.funcCode`, read by two more files) |
| `public/dashboard-app.js` | `FUNC_CODE` ×2, `BLOB_PROXY_CODE` |
| `public/welcome.html` | `FUNC_CODE` |
| `public/agentive_migration.html` | `FUNC_CODE` (a second, different key) |
| `public/admin.html` | `CODE` |
| `public/cygenix-api.js` | `CYGENIX_API_CODE` |
| `public/data-quality.html` | `DATA_API_CODE` |
| `public/insights.html` | inline ×2 |
| `public/register.html` | `CODE` |
| `public/report.html` | `NARRATIVE_FUNCTION_KEY` (a third key) |

**Impact.** View source, copy the key, set `x-user-id` to another customer's email: their projects,
jobs, saved connection metadata, quality reports and subscription record were readable and writable.
`requireAdmin` reads the role from the record belonging to that same spoofable id, so the bypass
reached the admin endpoints — role grants and membership extension — as well.

**Why it was still open.** The Function App *does* have Entra verification
(`azure-function/src/entra-auth.js`, `enforceAuth`), on a deliberate two-phase rollout gated by
`REQUIRE_TOKEN_AUTH`. Phase 2 never landed because the front end never started attaching a token, so
enforcement could not be turned on without 401-ing every sync call. Tracked internally as
"diagnose and fix /api/data 500s and auth fail-open", still open.

**What has been done (this change).** Every browser call now goes through
`netlify/functions/data-proxy.js`, which verifies the Entra token with the same verifier the other
Netlify functions use, derives the identity from the **verified claims**, and calls the Function App
with a key held in the environment. Any `x-user-id`, `?userId=` or `?code=` the caller sends is
discarded. `tests/data-proxy.test.js` drives the handler and asserts what the upstream receives;
`tests/secret-exposure.test.js` stops a key or a self-asserted identity being reintroduced.

**What has NOT been done, and cannot be, in code.** Two actions on the Azure side:

1. **Rotate both host keys** (Portal → Function App → App keys). They were public; removing them from
   the client stops *new* exposure but revokes nothing already copied.
2. **Set `REQUIRE_TOKEN_AUTH=true`** in the Function App settings. The proxy now forwards the
   caller's token, so this is finally safe to switch on — and until it is, a holder of an old key can
   still call the Function App directly and spoof the header.

One tracked exception remains in the client: `BLOB_PROXY_CODE` in `dashboard-app.js`. The Drive's
blob relay streams file **bodies**, and routing those through a Netlify function would cap every
upload at 6 MB. It is also lower severity — the relay carries the *customer's own* container SAS per
request, so the key alone reaches nobody's storage. The fix is Entra auth on the relay
(an Azure-side change); `tests/secret-exposure.test.js` asserts it is the only one outstanding.

---

## 2. Shape of the system

Three tiers, two of them backends, and they do not share an auth model — which is how §1 happened.

```
Browser  (48 static pages, vanilla JS, no framework, no build step)
   │
   ├── /.netlify/functions/*     11 functions · Node · Entra JWT verified · Netlify Blobs
   │
   └── data-proxy ──▶ Azure Function App  22 functions · Node · Cosmos DB + Azure Blob
                        (identity: Entra token when present, else x-user-id — see §1)
```

There is **no relational application database and no ORM**, so the brief's `ASSUMPTION:` of one, and
its "every schema change ships with a down migration" convention, do not apply as written. Persistence
is document stores:

| Store | Holds | Keyed by |
|---|---|---|
| **Netlify Blobs** `proj-<sub>` | projects | the verified Entra `sub`, **per user** |
| **Netlify Blobs** `cygenix-org` | RBAC users, role assignments, connection classifications, the audit chain | organisation-wide |
| **Netlify Blobs** (drive) | the Drive's documents | per user |
| **Azure Cosmos DB** | jobs, settings, connections metadata, scripts, profiles, quality reports, subscriptions, job versions | `userId` (an email) |
| **Azure Blob Storage** | uploaded source files | the customer's own container, via a SAS they supply |

**Tenancy did not exist.** Both project stores keyed on the *individual user*
(`safeStoreName(userId)` in `projects.js`, `userId` as the Cosmos partition key). Two colleagues at
the same customer could not see the same project.

**Built since (WI-6).** `netlify/functions/lib/tenancy.js` adds the tenant, its members, its
invitations and its guardrail policy, all in the `cygenix-org` blob store beside the RBAC state.
The Netlify project store is re-keyed to `projt-<tenantId>`, with a lazy non-destructive
forward-migration out of `proj-<sub>`; migrated projects are marked *private* to their original
owner, because tenancy is what lets colleagues share and not licence to disclose what was written
before they were colleagues.

**The Cosmos side is NOT re-keyed, deliberately.** `userId` is the partition key on a dozen
containers, and a partition key cannot be changed in place — it needs new containers and a copy of
every document. The boundary is enforced at the proxy instead: `data-proxy.js` derives `userId`
from the verified token and strips any the caller supplied, so cross-user reads were already
impossible there. What Cosmos still cannot do is *share* — a job or a saved script belongs to the
person who made it, not the tenant. That is the remaining half of this work item and it is a data
migration, not a code change.

---

## 3. Identity

**One provider: Entra External ID (CIAM)** — now genuinely, though this document's first pass
overstated it.

> **Correction.** The original claim here was that the brief's `ASSUMPTION:` of a split with Netlify
> Identity was wrong for the code, on the evidence of `grep -rl netlifyIdentity public/ netlify/`
> returning nothing. That grep was too narrow. Netlify Identity's API is **GoTrue**, and searching
> for that name found a live path: `admin.html` → `/api/data?action=invite` → the Azure Function's
> `invite` case → `POST https://<site>/.netlify/identity/admin/users` with a `NETLIFY_TOKEN`. The
> brief was right and this document was wrong. Inviting a teammate really did create them a second
> account in a second directory, and the Function App really did hold a long-lived site token to do
> it. That path is removed as part of WI-6, `invite` is out of the proxy's allow-list, and
> `tests/route-authz.test.js` fails the build if `gotrue`, `netlifyIdentity` or `/.netlify/identity`
> reappears in `netlify/`, `public/` or `azure-function/src` outside a comment.

- Tenant `fc8dfc7a-…`, authority `cygenix.ciamlogin.com`, MSAL browser 3.27.0 from cdnjs.
- `public/cygenix-auth-token.js` reads MSAL's `IdToken` record out of `localStorage`, memoised until
  30s before expiry, and exposes `getCygenixIdToken()`.
- Netlify side: `lib/entra-auth.js` verifies signature against JWKS, audience and issuer. Used by
  `projects.js`, `rbac-admin.js`, `reports.js`, `scheduler.js`, `drive.js` and now `data-proxy.js`.
- Azure side: `src/entra-auth.js`, same libraries, **enforcement off by default** (§1).

So WI-6's "exactly one identity provider remains in the codebase" is satisfied **now**, and was not
before — the consolidation work was real, just smaller than the brief imagined, because only one
flow used the second provider.

---

## 4. Authorisation — substantially built, and better than the brief describes

The brief says "two roles configured manually in an identity provider's admin UI is not RBAC". That
describes an older state. What is in `netlify/functions/lib/rbac.js` today:

- **Ten roles** — Organisation Owner, Platform Administrator, Migration Lead, Migration Engineer,
  Approver, Data Owner, Validator, Auditor, Member, Automation Principal — each mapped to an Entra
  app role.
- **A permission matrix as data**, ~30 actions × roles, with grants `F`/`L`/`A`/`R` and
  deny-by-default for anything absent.
- **Environment classification** per connection (DEV/TEST/STAGING/PROD), defaulting to PROD, with
  rules that bar an Engineer from PROD writes.
- **Separation of duties**, evaluated per act, with collapsed segregation surfaced rather than hidden.
- **First-signer bootstrap** so a new organisation is not locked out.

**A hash-chained audit log already exists**, which is most of WI-2:
`entry_hash = sha256(prev_hash + canonical entry)`, chained per organisation, append-only in
`audit/e/<seq>` with a moving `audit/head`, a `verifyEntries` walker that reports the first break, and
an export path. `rbac-admin.js?what=audit&verify=1` is the verification endpoint the brief asks for.

**Gaps against WI-2 as specified:** no AI-provenance columns (`ai_model`, `ai_prompt_hash`,
`ai_confidence`, `ai_disposition`), no shared redactor with its own test suite, no PDF export, and
the mapping/conversion/reconciliation/server-object event categories are not yet emitted. The
*mechanism* is done; the *coverage* is not.

**Honest caveat, already documented in the source:** Netlify Blobs has no transactions, so a
simultaneous append can race the head. A short retry closes the realistic window at current traffic
and `verifyChain` reports any break rather than hiding it. That is a real limit on the
"tamper-evident" claim under concurrency and should be stated in any evidence pack.

---

## 5. Client-side storage

~60 distinct keys. Classified in full in `docs/storage-inventory.md`. Summary against the brief's
A/B/C scheme:

- **Class A (authoritative)** — `cygenix_jobs`, `cygenix_projects`, `cygenix_project_settings`,
  `cygenix_sql_scripts`, `cygenix_validation_sources`, `cygenix_wasis_rules`, `cygenix_inventory`,
  and others. These **do** have a server source of truth: `cygenix-cosmos-sync.js` mirrors a declared
  `SYNC_KEYS` list to Cosmos with a per-field merge strategy, triggered by a monkey-patch on
  `localStorage.setItem`. So the brief's core fear — "clearing browser data destroys customer work" —
  is **mitigated but not eliminated**: the local copy is written first and syncs 3s later, so work
  created and then wiped inside that window is lost, and any Class A key *not* in `SYNC_KEYS` is
  local-only. WI-1 remains real, but it is a narrowing exercise, not a from-scratch build.
- **Class B (cache)** — schema snapshots (`cygenix_schema_*`), evidence maps, geo indices. Correct as
  they are.
- **Class C (preference)** — panel collapse state, sort order, theme, active tab. Correct as they are.
- **Secrets in Class A storage** — `cygenix_conn_string`, `cygenix_src_conn_string`, `cygenix_fn_key`,
  `anthropic_api_key`. This is WI-10's contradiction, live: the public claim is "credentials live in
  your browser, not on our servers", but scheduled jobs need credentials server-side, and both paths
  currently exist.

---

## 6. Execution, AI and tests

**Job execution.** Two paths. The browser-driven runner in `public/project-builder-app.js` stages
through `dm_staging.[schema__table]` with seven `dm_*` audit columns, then does a set-based
`INSERT … SELECT` into the target — so the *target-side* load is already set-based. The bottleneck the
brief identifies is real but sits one hop earlier: the browser→staging hop emits multi-row
`INSERT … VALUES` batches over `db-connect`. `SqlBulkCopy` is not in use anywhere. The scheduled path
runs in `azure-function/src/run-migration.js`.

**Resumability exists**: `public/cygenix-resume.js` plus per-run staging state; `tests/resume.test.js`
covers it. WI-7's checkpointing is therefore partly built.

**AI.** Model resolution is runtime and configurable (`public/cygenix-model.js`, a primary plus a
fallback chain, settable in Settings → Co-Worker). The brief's `ASSUMPTION:` of "Haiku, one-shot,
three retries" is out of date — the default primary is a Sonnet-class model and the chain is user-
editable. What the brief gets right: there is **no verification pass**, no prompt-hash caching, and no
fix propagation. WI-5 is genuinely unbuilt.

**Tests.** 62 files, **3,204 checks**, plain Node, no framework. Coverage is strong on engines and
weak on integration — there is no test against a real SQL Server anywhere.

**They did not run in CI.** The only workflow deployed the Azure Function. `.github/workflows/test.yml`
(added with this change) now runs the suite on every push and pull request, and fails on stale
generated assets.

---

## 7. The brief's assumptions, corrected

| Brief says | Reality | Consequence for the plan |
|---|---|---|
| A backend database exists and is the system of record | Two **document** stores (Netlify Blobs + Cosmos). No relational DB, no ORM, no migration tool | WI-1's table shapes and WI-2's DDL need re-expressing as documents. "Every migration has a down migration" does not apply |
| Some state lives only in `localStorage` | Class A keys **are** synced to Cosmos via a declared key list, with a 3s write-behind window | WI-1 shrinks to: close the window, cover the unsynced keys, add server-side retention |
| Identity is Entra with residual Netlify Identity | **The brief was right.** Entra for sign-in, but the invite flow still wrote to Netlify Identity's GoTrue admin API with a site token | WI-6's consolidation was real. Removed: one Azure Function action, one proxy allow-list entry, the admin page's invite flow |
| RBAC is two roles set by hand in an IdP console | Ten roles, a permission matrix, SoD, environment gating, all server-side | WI-6 is largely built. What is missing is **tenancy** and per-route coverage tests |
| Audit trail is claimed but absent | Hash-chained, append-only, with a verification endpoint | WI-2's mechanism exists. Missing: AI provenance, a redactor, PDF export, event coverage |
| AI mapping is Haiku, one-shot, retry-on-overload | Runtime model resolution with a configurable chain; still one-shot, still no verification | WI-5 stands, minus the model-routing item |
| Connection details supplied per-request from the browser | Correct, and connection strings are also persisted to `localStorage` | WI-10's contradiction is real and live |
| Insert batch capped at 1,000 rows | Staging load is batched `INSERT … VALUES`; staging→target is already set-based | WI-7 stands, but the target is the browser→staging hop |
| — *(not in the brief)* | **Host keys published in the client; identity from a request header** | Outranked everything. Fixed in code; **needs key rotation and one app setting** |
| — *(not in the brief)* | **The test suite never ran in CI** | Fixed |
| — *(not in the brief)* | **Project stores are keyed per user, not per tenant** | WI-6 is bigger than scoped: a re-keying, not a new column. Done for Netlify Blobs; Cosmos needs a container rebuild |
| — *(not in the brief)* | **The console's own project record was ungated** — any signed-in caller could read and write it | Fixed: `project.read` / `project.write` rows added to the matrix |

---

## 8. Suggested revision to the sequencing

The brief's Phase 1 is WI-1 → WI-2 → WI-9. Given the above:

0. **Rotate the keys, set `REQUIRE_TOKEN_AUTH=true`.** Not engineering work. Nothing else matters
   until it is done.
1. ~~**WI-9 (capability manifest)**~~ — **done.** The manifest, the generated support matrix, four
   sales/code contradictions closed, and target-edition gating in Server Migration.
2. ~~**WI-6 tenancy**~~ — **done for the Netlify layer.** Tenant, membership, invitations,
   server-enforced guardrails, the two-person rule, cross-tenant 404 with an audit entry, and route
   coverage tests. Two pieces remain and are called out above: the Cosmos containers are still
   partitioned on `userId` so jobs and scripts are not yet shareable within a tenant, and
   `admitJob()` from WI-9's manifest is still not called by any server endpoint.
3. **WI-1 narrowed** — close the write-behind window, cover the unsynced Class A keys, add
   server-side retention. Not a from-scratch build.
4. **WI-2 coverage** — the chain exists; add AI provenance, the redactor and the missing events.
5. Then WI-4 → WI-3 as the brief has it. Those are the differentiators and they are genuinely unbuilt.

---

*Every claim here was read from the repository at commit-time. Where this document and the brief
disagree, this one was checked against code and the brief was not.*
