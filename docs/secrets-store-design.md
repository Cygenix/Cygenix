# Server-side secrets store — design note

**Phase 2. No code has been written. This is for review.**
Read `docs/credential-storage-audit.md` first; this note assumes its findings.

---

## What this has to be true of

Restating the requirements as things that must hold, because two of them turn out to
decide the design on their own:

1. Secrets are written from the UI and never read back into it. The client holds an
   opaque reference plus non-sensitive metadata.
2. Encrypted at rest with a key we control, scoped per tenant.
3. UK data residency holds, demonstrably.
4. Access gated by the existing ten RBAC roles, including a path for the Automation
   Principal with no human session.
5. Every create, rotate, use and delete writes to the hash-chained audit trail —
   who or what, when, which secret, never the value.
6. Rotation and revocation without a redeploy, and without breaking a running stream.
7. Redacted in logs, errors and telemetry **by construction**, not by discipline.

Requirements 5 and 6 are the ones that choose the architecture. Everything else is
satisfiable either way.

---

## Recommendation

**Envelope encryption in Cosmos DB, with the key-encryption key in Azure Key Vault.**

Per-tenant data-encryption keys (DEKs), each wrapped by a tenant-scoped key-encryption
key (KEK) held in Key Vault. Ciphertext, wrapped DEK and metadata live in a new Cosmos
container. Key Vault is reached with the managed identity the Function App already has.

Not Key Vault as the direct store. The reasoning is below, and it is mostly requirement
5.

---

## The two approaches, compared

### A — Key Vault as the direct store

One Key Vault secret per credential. `data-proxy` or the Function App fetches on use.

**For it.** Least code we write ourselves. HSM-backed. Rotation, versioning, soft
delete and purge protection are Azure's problem. Access policy is Azure RBAC, audited
in Azure Monitor whether or not we do anything. It is the answer a security
questionnaire likes hearing, and that has real commercial value.

**Against it, in order of how much it matters:**

- **The audit trail splits in two.** Requirement 5 wants every *use* in our
  hash-chained log, attributed to a role or an Automation Principal. Key Vault's own
  audit is in Azure Monitor, keyed by Azure identity, not by our ten roles — so the
  entry that matters to a customer's evidence pack is one we have to write anyway, at
  the call site, on top of Azure's. Two logs that must agree is the failure mode the
  chain exists to avoid. We would end up writing our own entries regardless, which
  removes most of the "Azure does it for you" argument.
- **Latency on every use.** A Key Vault fetch is a network round trip, typically
  20–80 ms warm, more on a cold managed-identity token. Fine for a migration that
  runs once. Not fine for a Data Stream reconnecting per batch, which is precisely the
  always-on direction the roadmap is heading. Caching fixes the latency and
  reintroduces the revocation problem (requirement 6) — a cached secret survives a
  revoke until its TTL expires.
- **Throttling and cost scale with secret count and traffic.** Key Vault has per-vault
  request limits, and one vault holding thousands of tenant secrets becomes a single
  throttling domain. Splitting into per-tenant vaults fixes that and multiplies the
  provisioning burden.
- **Blast radius is not obviously better.** Compromise the managed identity and you can
  read every secret the identity is allowed to read — which, in a single-vault design,
  is all of them. Envelope encryption has the same weakness, so this is a wash rather
  than a point against.

### B — Envelope encryption in our own store, KEK in Key Vault  ← recommended

Per credential: generate a random DEK, AES-256-GCM the plaintext, wrap the DEK with the
tenant's KEK via Key Vault's `wrapKey`, store `{ciphertext, iv, tag, wrappedDek,
kekVersion, metadata}` in Cosmos. To use: `unwrapKey` the DEK, decrypt, use, discard.

**For it.**

- **One audit trail.** The decrypt happens in our code, so the audit entry is written
  in the same function, in the same transaction of thought, and cannot drift from the
  event it records. Requirement 5 falls out of the design instead of being bolted onto
  it.
- **Revocation is a delete of a row we own**, immediate and complete, with no cache TTL
  to wait out. Requirement 6 becomes easy rather than delicate.
- **Key Vault sees `wrapKey`/`unwrapKey` traffic on a handful of KEKs**, not thousands
  of secret reads. The KEK can be cached — it is wrapped material, not plaintext — so
  steady-state latency is a Cosmos read plus an in-process AES operation, which is
  sub-millisecond after the first unwrap.
- **Per-tenant scoping is structural.** One KEK per tenant means tenant A's wrapped DEK
  is undecryptable with tenant B's key, and that is enforced by cryptography rather
  than by a query filter somebody might forget. This is the strongest argument of the
  lot for a multi-tenant product making residency and isolation claims.
- **It reuses what exists.** Cosmos is already there, already partitioned, already
  backed up, already in UK South. `@azure/identity` is already a dependency and
  `DefaultAzureCredential` is already in use for SQL.

**Against it.**

- **We write and own the crypto envelope.** That is real risk. It is bounded — AES-GCM
  via Node's `crypto` with a random 96-bit IV per encryption and no key reuse across
  tenants is a well-trodden path, not novel cryptography — but it is code that must be
  reviewed and tested as security code rather than as a feature.
- **More to build.** Key generation, wrapping, rotation, re-wrap on KEK rotation, and
  the migration of existing data. Approach A gets some of that free.
- **A KEK rotation requires re-wrapping every DEK for that tenant.** Straightforward,
  but it is a background job somebody has to write and monitor.

### The trade-off stated plainly

| | A — Key Vault direct | B — Envelope, KEK in Vault |
|---|---|---|
| Cost | Per-operation, scales with *uses* | Per-operation on wrap/unwrap only; scales with *secrets* |
| Latency per use | 20–80 ms, or cache and weaken revocation | One Cosmos read + in-process AES |
| Blast radius | Identity compromise → all readable secrets | Identity compromise → all unwrappable DEKs. Comparable. Per-tenant KEKs limit a *key* compromise to one tenant |
| Operational burden | Low; Azure runs it | Moderate; we run rotation and re-wrap |
| Audit (req. 5) | **Split across two systems** | **Single chain, written where the event happens** |
| Revocation (req. 6) | Cache TTL vs latency trade-off | Immediate |
| Code we own | Little | The envelope, and it is security code |

B wins on the two requirements that were stated as hard, and loses on the one thing we
can most readily mitigate — reviewable, testable code.

---

## The shape of it

### Storage

A new Cosmos container `secrets`, partitioned on `tenantId` (not `userId` — this is
tenant-scoped from the start, and does not inherit the per-user partitioning the rest
of Cosmos is stuck with).

```
{
  id, tenantId,
  ref,                          // "sec_01J…" — the opaque handle the client holds
  kind,                         // 'connection' | 'connector' | 'api-key'
  name, createdAt, createdBy,
  lastUsedAt, lastRotatedAt,    // metadata the UI may show
  ciphertext, iv, tag,          // AES-256-GCM
  wrappedDek, kekName, kekVersion,
  revokedAt
}
```

`ref` is what leaves the server. Everything from `ciphertext` down is projected away by
the read path — not filtered at the call site, but removed in one `toClientShape()`
that every response goes through, so requirement 1 holds structurally.

### The API surface

Through `data-proxy`, added deliberately to its `ALLOWED` set as every action is:

| Action | Roles | Notes |
|---|---|---|
| `secret-create` | Owner, Platform Admin, Migration Lead, Data Owner | Returns `ref` + metadata. Plaintext in the request body, never in the response. |
| `secret-list` | any role with the resource's read grant | Metadata only. |
| `secret-rotate` | as create | New value, same `ref`. In-flight users keep the old version until they finish — see below. |
| `secret-revoke` | Owner, Platform Admin, Data Owner | Immediate. |
| `secret-use` | **not exposed** | There is no read endpoint. Decryption happens server-side inside the job/stream that needs it. |

That last row is the important one. **The absence of a read endpoint is the mechanism**
for requirement 1 — not a rule about how to call the API.

### Automation Principal

The Automation Principal holds an API key (Prompt 3's work) mapped to that role. A
scheduled run resolves its secret by `ref` under the Automation Principal's grants, with
the audit entry attributed to the key rather than to a person. Two constraints worth
writing down now:

- An Automation Principal may **use** a secret and may not **create, rotate or read
  metadata about** one. Automation that can mint credentials is a privilege-escalation
  path into the two-person rule.
- Its uses are audited identically to a human's, with `actor_kind: 'automation'` so an
  auditor can tell them apart without having to know which names are people.

### Rotation without breaking a running stream

Version the wrapped DEK, not just the secret. A rotate writes a new version and marks
the previous one `supersededAt` with a grace window (propose 24 hours). A stream that
opened its connection with version 3 keeps working; its next reconnect takes version 4.
A revoke, unlike a rotate, has no grace window — that is the difference between the two
operations and the reason both exist.

### Redaction by construction

One shared module, `netlify/functions/lib/redact.js`, with its own test suite, and:

- Every secret value is wrapped in a `Secret` box whose `toString()`, `toJSON()` and
  `util.inspect.custom` all return `'***'`. A `console.log` of a config object
  containing one prints `***`, without the call site knowing. Unwrapping is an explicit
  `.reveal()` call, and `.reveal()` is greppable — which makes review tractable.
- The existing per-file redactors (`send-email.js`, `connectors.js`,
  `cygenix-privacy.js`, and four others) collapse into it.
- A test asserts that a `Secret` cannot be JSON-serialised into a response body.

This is the requirement most often satisfied by intention and least often by
construction, so it is worth the box type.

### Residency

Cosmos in UK South (as now), Key Vault provisioned in UK South, Function App in UK
South. Nothing crosses a region because nothing is replicated out of it — but that must
be **asserted in configuration**, not assumed: Cosmos geo-replication off or pinned to
UK West for failover only, and a startup check that refuses to run against a vault
whose URI is not `*.vault.azure.net` in a UK region. A residency claim that depends on
nobody ticking a box in the Portal is not a claim.

---

## Migrating what is already in browsers

Requirement: a one-time prompted import, then a verified purge.

1. **Detect.** On sign-in, if any class-S key is present, show a one-time panel: "Your
   connection details are stored in this browser. Move them to your Cygenix account?"
   List what was found by name, never by value.
2. **Import.** Each accepted item is POSTed to `secret-create` and comes back as a
   `ref`. The project's connection metadata is rewritten to hold the `ref`.
3. **Verify, then purge.** Only after the server confirms the `ref` resolves does the
   client `removeItem()` the original key. Purge-then-fail is the mistake
   `resetToCloud()` made and the same rule applies: **verify before you destroy.**
4. **Decline** leaves everything working exactly as today, with the panel re-offered
   once per 30 days rather than on every load.

**Someone who never opens the app again** keeps their credentials in their own browser
until they clear it, and we hold nothing. That is the status quo continuing, not a new
exposure — the important part is that we do not silently keep a copy, and that a
scheduled job belonging to that user cannot run, because it never had server-side
credentials to run with. Which is the honest answer, and is worth stating on the
Integrations page: **scheduled jobs require an imported credential.**

The wording on the Integrations tab has to change either way. Suggested:
credentials you import are encrypted with a key held for your organisation and never
returned to the browser; credentials you do not import stay in your browser and cannot
be used by scheduled jobs.

---

## What I would deliberately leave out of v1

- **Bring-your-own-key (customer-managed KEK).** The Enterprise tier already advertises
  "customer-managed encryption keys" and nothing implements it (see
  `docs/pricing-claims-audit.md`). The design above makes it *possible* — point a
  tenant's KEK at their vault — but doing it properly means handling a customer
  revoking our access, which is a support problem before it is a code problem.
- **Secret sharing across tenants.** No.
- **A UI for browsing secret versions.** Metadata and a rotate button is enough for v1.
- **Migrating `cygenix_api_key`.** It is the customer's own third-party key, the
  exposure is theirs, and it is written from twelve sites. Worth doing, not worth
  doing first.

---

## The reference connector, when we get to Phase 3

**SMTP**, and the choice is not close.

It is the only connector whose credential *already* crosses to a server, so it is the
one place where moving the storage changes the trust story without changing the data
flow. `send-email.js` already redacts by construction and persists nothing, so it is
the natural first consumer of the `Secret` box. It has a real password rather than a
URL. And it is small.

Webhook would be easier still, but its "secret" is an optional HMAC shared secret that
never leaves our side anyway, so migrating it proves less.

---

## Open questions blocking Phase 3

1. **Is a Key Vault provisioned, and in which region?** If not, that is the first
   provisioning step and it is not a code change.
2. **Does the Function App have a managed identity assigned**, system- or
   user-assigned? `tests/sql-entra.test.js` shows the code supports both.
3. **Is Cosmos geo-replication currently on, and to where?** This decides whether the
   residency claim holds today.
4. **Do we want per-tenant KEKs from day one**, or one KEK with per-tenant DEKs and
   per-tenant KEKs later? Day one is cleaner and slightly more provisioning.

---

**Stop here.** Phase 3 is implementation behind a feature flag, with the tests the
prompt lists, and only once this note is approved.
