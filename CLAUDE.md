# CLAUDE.md

Read `docs/architecture-as-built.md` next — it is longer, it was also read from the
code, and it corrects several claims made about this system from outside it.

## What this is

Cygenix is a SQL migration console: a browser app that moves schema, data and
server-level objects between SQL Server, Azure SQL and PostgreSQL. Around the
migration sit an AI column-mapping step, a preflight/validation engine, a Task Agent
that runs jobs on cron and chains them on success, a Data Stream module (CDC), and a
governance layer with a hash-chained audit trail, a two-person rule on destructive
operations and ten RBAC roles.

Hosted on Netlify + Azure, UK data residency. Tiers £99 / £299 / £799 plus Enterprise.

Strategic direction: extending from one-off migration into ongoing data integration —
keeping a migrated estate in sync with the systems around it. Judge design decisions
against that, not only against migration.

**Main journeys:** connect a source and target → explore the schema → map objects and
columns (AI-assisted) → preflight/validate → run the migration → read the conversion
report. Alongside: schedule and chain jobs, run data-quality assurance, plan effort
and project timelines, and stream changes continuously.

## Stack

- **No framework, no build step, no bundler, no TypeScript.** 48 static HTML pages in
  `public/`, each with inline `<style>` and `<script>`, plus 69 shared vanilla-JS
  modules loaded by `<script src>`.
- **CSS custom properties** for theming. `public/cygenix-brand.css` holds the tokens
  shared across public pages; `public/cygenix-theme.css` holds the *alternative named
  themes* a signed-in user can pick. They are not interchangeable.
- **Tests are plain Node**, no framework, no assertion library: each file counts its
  own passes and `process.exit(1)`s on failure. `npm test` chains ~74 of them with
  `&&`, so it stops at the first failure. Browser smokes use `playwright-core`
  against the preinstalled Chromium (`npm run test:browser`).
- **npm**, Node ≥ 18.

## Architecture

Three tiers; two of them are backends, and they do not share an auth model.

```
Browser  48 static pages, vanilla JS
   │
   ├── /.netlify/functions/*     11 Node functions · Entra JWT verified · Netlify Blobs
   ├── /api/* edge functions      5 Deno edge functions
   │
   └── data-proxy ──▶ Azure Function App   23 Node functions · Cosmos DB + Azure Blob
```

`netlify/functions/data-proxy.js` is **the only route from the browser to the Azure
Function App.** It verifies the Entra token, derives the identity from the verified
claims, and calls the Function App with a host key held in the environment. Any
`x-user-id`, `?userId=` or `?code=` the caller sends is discarded. Adding an action
means adding it to that file's `ALLOWED` set — deliberately, not by pass-through.

Routing: `public/_redirects` is **generated** by `scripts/build-routes.js`. Do not edit
it. Extensionless URLs (`/dashboard`, not `/dashboard.html`) are the public form.

## Persistence

| Store | Holds | Keyed by |
|---|---|---|
| Netlify Blobs `projt-<tenantId>` | projects | tenant |
| Netlify Blobs `cygenix-org` | RBAC state, connection classifications, the audit chain | organisation |
| Azure Cosmos DB | jobs, settings, connection metadata, scripts, profiles, quality reports, subscriptions, job versions | `userId` (an email) |
| Azure Blob Storage | uploaded source files | the customer's own container, via a SAS they supply |

**No relational application database and no ORM.** Both stores are document stores, so
"every schema change ships with a down migration" does not apply here.

**Browser storage is load-bearing and partly authoritative.**
`public/cygenix-cosmos-sync.js` mirrors a declared `SYNC_KEYS` list to Cosmos on a 3s
write-behind, triggered by a monkey-patch on `localStorage.setItem`. Every key is
classified in `docs/storage-inventory.md`, which is **generated** by
`scripts/storage-inventory.js`; `tests/storage-inventory.test.js` fails if a key is
added without a classification. Seven keys are classed **S — Secret**, including
plaintext connection strings. That is a known finding, not a design decision — see
`docs/credential-storage-audit.md`.

## Auth, tenancy, RBAC

- **One identity provider: Entra External ID (CIAM)**, authority `cygenix.ciamlogin.com`,
  MSAL browser 3.27.0. `public/cygenix-auth-token.js` finds MSAL's `IdToken` credential
  record in `localStorage` and exposes `getCygenixIdToken()`.
- **Server verification:** `netlify/functions/lib/entra-auth.js` (signature against
  JWKS, audience, issuer) and `azure-function/src/entra-auth.js`. **Azure-side
  enforcement is off by default** behind `REQUIRE_TOKEN_AUTH` — see Open questions.
- **Tenancy:** `netlify/functions/lib/tenancy.js` — tenants, members, invitations,
  guardrail policy. The Netlify project store is keyed per tenant. **Cosmos is still
  partitioned on `userId`**, so jobs and scripts are not yet shareable within a tenant.
- **RBAC:** `netlify/functions/lib/rbac.js`. Ten roles — Organisation Owner, Platform
  Administrator, Migration Lead, Migration Engineer, Approver, Data Owner, Validator,
  Auditor, Member, Automation Principal — each mapped to an Entra app role. A
  permission matrix as data (~30 actions × roles, grants `F`/`L`/`A`/`R`),
  deny-by-default. Environment classification per connection (DEV/TEST/STAGING/PROD,
  defaulting to PROD). Separation of duties evaluated per act.

## The audit trail

In `netlify/functions/lib/rbac.js`, in the `cygenix-org` blob store. Append-only at
`audit/e/<seq>` with a moving `audit/head`. Each entry chains:

```
entry_hash = sha256(prev_hash + canonical(entry))
```

`verifyEntries` walks the chain and reports the first break;
`rbac-admin.js?what=audit&verify=1` is the verification endpoint. **Known limit:**
Netlify Blobs has no transactions, so a simultaneous append can race the head. A short
retry closes the realistic window and `verifyChain` reports a break rather than hiding
it — state that in any evidence pack rather than claiming unqualified tamper-evidence.

Missing coverage: no AI-provenance fields, no shared redactor, no PDF export, and the
mapping/conversion/reconciliation/server-object event categories are not yet emitted.

## Running, testing, deploying

```bash
npm install
npm test                 # ~74 suites, plain Node, stops at first failure
npm run test:browser     # 7 Playwright smokes against preinstalled Chromium
```

There is no dev server script. Serve `public/` statically for UI work; anything
touching `/.netlify/functions/*` needs `netlify dev` or a deploy.

Deploy is Netlify on push to `main`, build command in `netlify.toml`:

```
npm install && node scripts/check-env.js && node scripts/stamp-assets.js && node scripts/build-routes.js
```

The Azure Function App deploys via `.github/workflows/main_cygenix-db-api.yml`.
`.github/workflows/test.yml` runs the suite on every push and PR.

## Conventions a first PR gets wrong

1. **Run `node scripts/stamp-assets.js` after editing anything in `public/`.** Every
   `<script src>` carries a content hash in its query string;
   `tests/asset-stamping.test.js` fails on a stale stamp.
2. **Three files are generated** — `public/_redirects`, `docs/storage-inventory.md`,
   and the asset stamps. Re-run the script, never hand-edit.
3. **No emoji anywhere in `public/`.** `tests/icons.test.js` bans several Unicode
   ranges against a small KEEP set. Use the `ic-*` icon classes.
4. **Comments explain *why*, at length.** This codebase's house style is a long header
   comment on each module saying what went wrong before and what the code is
   defending against. Match it; do not strip it.
5. **Add a test in the same style** — plain Node, its own `check()` counter, exit
   non-zero on failure — and wire it into the `test` chain in `package.json`.
6. **Never reintroduce a secret or a self-asserted identity into the client.**
   `tests/secret-exposure.test.js` fails the build if you do.
7. **Errors are classified, not swallowed.** See `public/cygenix-data-api.js`
   (`classify`, `callResult`) — `null` must never mean both "failed" and "empty".
8. **Keep `CygenixSync`'s public API backwards compatible.** ~40 scripts depend on it;
   `load()`/`forceLoad()` return booleans. Add alongside rather than repurposing.

## Open questions

Answer these and I will fold them in.

1. **Are the two Azure Function host keys rotated, and is `REQUIRE_TOKEN_AUTH=true`
   set?** Both were published in the client. The code fix landed; these two Portal
   actions are what actually revokes the exposure. Tracked in
   `docs/DEPLOY-auth-fix.md` and still shown as outstanding.
2. **Is there an Azure Key Vault provisioned, and does the Function App have a
   managed identity assigned?** `@azure/identity` is a dependency of both backends and
   `DefaultAzureCredential` is used for SQL, so identity exists — but no Key Vault SDK
   is used anywhere, so I cannot tell whether a vault exists.
3. **Which Azure region are the Function App, Cosmos account and storage accounts
   actually in?** The hostname says `uksouth`; the residency claim needs the other two
   confirmed.
4. **Is there a staging environment**, or does `main` deploy straight to production?
5. **Who is the Automation Principal today** — is that role assigned to anything, or
   is it declared but unused?
6. **Which tier is a new sign-up given**, and where is that decided? I could not find
   the assignment path for `cygenix_tier`.
