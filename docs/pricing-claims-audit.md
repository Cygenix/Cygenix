# Pricing page claims, checked against the code

**Prompt 2, job 1.** Every claim on `public/pricing.html` against what exists. Nothing
on the pricing page was changed — the prompt asks for the table so the wording-versus-
building decision stays with you.

Verdicts: **True** — it works. **Partly true** — the capability exists but the tier
distinction does not, or it is beta/preview rather than GA. **Not built** — no.

---

## The finding that applies to nearly every row

**`public/cygenix-capabilities.js` declares every tier limit, and nothing enforces
them.** The manifest has the numbers exactly as advertised:

```js
{ id: 'starter',    concurrentMigrations: 1,    maxRowsPerJob: 1000000,   retentionDays: 30  }
{ id: 'pro',        concurrentMigrations: 3,    maxRowsPerJob: 10000000,  retentionDays: 90  }
{ id: 'business',   concurrentMigrations: 10,   maxRowsPerJob: 100000000, retentionDays: 365 }
{ id: 'enterprise', concurrentMigrations: null, maxRowsPerJob: null }
```

and `admitJob(tierId, job)` at `:406` implements the check correctly. **Its only caller
is its own test.** No server endpoint calls it — `docs/architecture-as-built.md` already
flagged this, and it is still true.

So every quantitative claim below is "partly true" in the same way: the number is right,
it is written down in the right place, and a Starter customer can run ten concurrent
100M-row jobs because nobody asks. That is a revenue leak rather than a false claim, but
it is the same fix in every row — call `admitJob` from the job-submission path — and it
is one piece of work, not eight.

---

## Starter — £99

| Claim | Actually | Verdict |
|---|---|---|
| 1 migration at a time | Declared in the manifest; not enforced | Partly true |
| Up to 1M rows per job | Declared; not enforced | Partly true |
| 30-day job history | Declared; no retention job exists to expire anything | Partly true |
| Migrate to SQL Server & Azure SQL | `mssql` and `azuresqldb` both `status: 'ga'` | **True** |
| AI column mapping | `ai_mapping`, `status: 'ga'`, `minTier: 'starter'` | **True** |
| Email support | Commercial, not code | n/a |

## Pro — £299

| Claim | Actually | Verdict |
|---|---|---|
| 3 migrations in parallel | Declared; not enforced | Partly true |
| Up to 10M rows per job | Declared; not enforced | Partly true |
| **Priority processing queue** | **No queue exists, prioritised or otherwise.** Grep for priority/queue semantics returns nothing. Jobs run when submitted | **Not built** |
| 90-day job history + version control | Versioning is real — `version-create`, `version-list`, `restore-version` in the proxy's allow-list, backed by Cosmos. The 90-day part is unenforced | Partly true |
| SQL Server, Azure SQL and Postgres | `postgres` is `status: 'ga'` on both source and target | **True** |
| Email notifications & daily digests | Email notifications are real (`azure-function/src/notify.js`, `netlify/functions/send-email.js`). **No digest exists** — no scheduled aggregation, nothing matching "digest" anywhere | Partly true |
| Priority email support | Commercial | n/a |

## Business — £799

| Claim | Actually | Verdict |
|---|---|---|
| 10 migrations in parallel | Declared; not enforced | Partly true |
| Up to 100M rows per job | Declared; not enforced. Also worth a look on its own merits — the browser→staging hop emits multi-row `INSERT … VALUES` batches and `SqlBulkCopy` is not used anywhere, so 100M rows is a claim about throughput nobody has measured | Partly true |
| **Dedicated job queue — no waiting** | No queue exists | **Not built** |
| 365-day retention + full version history | Version history real; retention unenforced | Partly true |
| MS SQL Server Migration (packages, logins, objects) | `server_objects`, `status: 'beta'`. Logins, server roles, Agent jobs, SSIS, linked servers and credentials are all declared in the manifest with individual statuses | Partly true — real, but beta |
| **Custom source / target connectors** | Nothing. No connector SDK, no adapter interface, no plugin surface. This is Prompt 6's entire scope | **Not built** |
| **Slack** & webhook notifications | Webhook is live (`cygenix-integrations.js:141,156`). **Slack is `status: 'coming-soon'`** at `:93`, sitting in the Contact Sales list | Half not built |
| SLA-backed support, 4-hour response | Commercial | n/a |

## Enterprise — Custom

| Claim | Actually | Verdict |
|---|---|---|
| Unlimited concurrent migrations | `concurrentMigrations: null`. Unenforced like the rest, which here means the claim is accidentally true | **True** |
| Unlimited row counts | Same | **True** |
| **Dedicated infrastructure — your own stack** | Nothing in the code. May be a deployment offering rather than a product feature — needs your answer, not mine | Unverifiable from the repo |
| Unlimited retention + audit trail | Audit trail is real and hash-chained (`netlify/functions/lib/rbac.js`). `audit_trail` is `status: 'beta'`, `minTier: 'business'`. Retention unenforced, so again accidentally true | Partly true |
| MS SQL Server Migration | As Business | Partly true |
| **BYO Azure subscription option** | Nothing in the code | Unverifiable / Not built |
| **Customer-managed encryption keys** | Nothing. No Key Vault SDK is used anywhere; there is no encryption-at-rest layer we control to point a customer key at. `docs/secrets-store-design.md` would make it possible, and explicitly leaves it out of v1 | **Not built** |
| Named CSM & SLA-backed support | Commercial | n/a |

---

## The not-built list, with effort estimates

Not building these, per the prompt. Estimates assume the secrets store has landed where
credentials are involved.

| Item | Tier | Estimate | Note |
|---|---|---|---|
| **Slack connector** | Business | **1–2 days** | Prompt 2's job 2. Blocked: needs the secrets store, or it adds a fourteenth localStorage credential |
| **Enforce tier limits** | all | **2–3 days** | `admitJob` already exists and is tested. The work is calling it from the submission path, plumbing the caller's tier, and deciding the failure UX. Fixes eight "partly true" rows at once |
| **Daily digest** | Pro | **2–3 days** | Needs a scheduled aggregation over job history and a template. `scheduler-run-background.js` is the natural host |
| **Retention enforcement** | Starter/Pro/Business | **3–5 days** | A scheduled purge over Cosmos plus the audit entries to prove it ran. Interacts with the audit trail's own retention, which is a policy question first |
| **Priority / dedicated queue** | Pro, Business | **2–3 weeks** | There is no queue at all today. This is a real architectural addition — durable queue, workers, fairness, admission control — not a flag |
| **Custom source/target connectors** | Business | **4–6 weeks** | Prompt 6's connector SDK plus at least one adapter |
| **Customer-managed encryption keys** | Enterprise | **2–3 weeks** *after* the secrets store | Mostly the operational half: what happens when a customer revokes our access to their key mid-migration |
| **BYO Azure subscription / dedicated infrastructure** | Enterprise | Unknown | Tell me whether these are product features or a professional-services offering; if the latter, they belong in different words on the page |

---

## What I would do about it

Three separable decisions, and I would take them in this order:

1. **The two claims I would reword this week, not build.** "Priority processing queue"
   and "Dedicated job queue — no waiting" describe an architecture that does not exist
   and would take a month to build. They are also the two claims a customer is least
   likely to miss if they are absent and most likely to notice if they are false.
2. **Slack is worth building** — it is a day or two and it is the cheapest of these to
   make true — but it is genuinely blocked behind the secrets store, and adding it to
   `localStorage` now would make the audit above worse rather than better. The prompt
   anticipated this and said to stop; I have.
3. **Enforcing the tier limits is the highest-value item on the list** and is not on it
   for a marketing reason. It closes eight rows, the code is already written and
   tested, and until it lands the tiers are advisory.
