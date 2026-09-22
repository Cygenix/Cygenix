# Server-held connections, bound to profiles

**Status: design only. Nothing in this document is built.** It waits for the
owner's confirmation. Phase 1 (commit `112564f`) made the current model fail
loudly and fetch the product's own Function App key; this is what replaces
that model.

Written from the code as it is on 22 Sep 2026. Where a claim depends on
something I could not see from the repository (an Azure resource, a Portal
setting) it says so.

---

## 1. The problem, in one paragraph

A saved connection is two halves. The name, side, mode and endpoint sync to
Cosmos with everything else. The credential — the password inside the
connection string, or the Function App key — lives in the browser that typed
it, in `cygenix_saved_conn_secrets`, keyed to one person, and reaches the
cloud only through a per-user encrypted store that (until Phase 1) had never
worked. So a credential is a thing each person types, on each machine, again
after every cookie clear, and a Function App key is a thing a person has to
*know*. The owner's instruction for this design is the one that settles the
shape: **connections are shared org-wide.** A connection belongs to the
organisation, is bound to a Connection Profile, and anyone with rights to
that profile runs with it and never sees or re-enters its secret.

That is Boomi's model, and it is the right one for a product that is moving
from one-off migration into ongoing integration: a schedule that runs at
03:00 cannot depend on a password in somebody's laptop.

## 2. The split: metadata and secret

Every connection becomes two records that share an id.

**Connection (metadata)** — synced as today, readable by anyone who can see
the organisation's register.

```json
{
  "id": "conn_01J9…",
  "tenantId": "t_…",
  "name": "Finance prod source",
  "side": "src",
  "kind": "sqlserver | postgres | azurefn",
  "server": "sqlprod01.internal", "port": 1433, "database": "Conversion_DM",
  "authType": "sql | entra | key",
  "userName": "svc_cyg_fin",
  "envClass": "PRD",
  "secretRef": "sec_01J9…",
  "secretUpdatedAt": "2026-09-22T10:00:00Z", "secretUpdatedBy": "admin@…",
  "createdAt": "…", "createdBy": "…", "retiredAt": null
}
```

The connection string is never stored whole. It is *assembled* on the server
from metadata plus the secret at the moment of use. That is what makes the
metadata safe to sync and safe to show.

**Secret** — stored only on the server, encrypted, referenced by `secretRef`.

```json
{ "id": "sec_01J9…", "tenantId": "t_…", "connId": "conn_01J9…",
  "kind": "password | connString | fnKey",
  "iv": "…", "ct": "…", "tag": "…", "wrappedDek": "…", "kekVersion": "kv-key/v3",
  "updatedAt": "…", "updatedBy": "…" }
```

`kind: connString` exists for the migration period and for engines whose
string cannot be assembled from parts. It is the whole string, sealed; the
server still never returns it.

### Where each half lives

| Half | Store | Why |
|---|---|---|
| Connection metadata | Cosmos, container `org_connections`, partition `/tenantId` | Org-scoped, not per-user; the profile store already refers to connections by id and merges by record |
| Secret | Cosmos, container `org_secrets`, partition `/tenantId` | Only the Function App reads it; the row is ciphertext plus a wrapped key |
| Bindings and profiles | `cygenix_profiles_v1` as today, `connMeta` retired into the connection record | Profiles already bind by connection id |
| Roles | Netlify Blobs `cygenix-org` as today | That is where roles are decided |

The current per-user `conn_secrets` container (keyed on the Entra oid) is
the wrong scope for an org-owned connection and is retired by the migration
in §10.

## 3. Write-only secrets

A secret goes **in** through one endpoint and never comes **out**.

- Save: `PUT /api/connections/{id}/secret` with the plaintext in the body over
  TLS, verified Entra token, role checked (§7). The server seals it and stores
  it. The response is `{ secretUpdatedAt, secretUpdatedBy }` and nothing else.
- Show: the Connections page and the Profiles register show
  **"Password saved · updated 22 Sep 2026 by admin@…"** with a **Replace**
  button. Replace opens a field, sends a new secret, and the line updates.
  There is no Reveal, no Copy, and no way to read the old value back.
- Test: **Test** sends `{ connId }` only. The server resolves and connects. A
  wrong password comes back as the driver's error, which is enough.

The UI already draws a masked row for the live pair; this replaces the mask
with a fact about the server rather than a hidden copy of the value.

## 4. Server-side resolution

The browser stops sending connection strings and keys. Every run request
carries **`profileId` and `role` (`SRC` or `TGT`)** and nothing about the
endpoint. The server:

1. verifies the Entra token and the grant (§7);
2. loads the profile, takes `srcConnId`/`tgtConnId` for the role;
3. checks the profile is active and the connection is not retired;
4. loads the connection metadata and the secret, unwraps the key, decrypts;
5. assembles the driver config, connects, runs the action, discards the
   plaintext;
6. records the run (§8).

### Which host resolves

**The Azure Function App, for all database work.** Three reasons, and they
are the brief's own constraints:

- **Managed identity.** The Function App already depends on `@azure/identity`
  and uses `DefaultAzureCredential` for Entra SQL auth. Key Vault access
  through the same identity is one role assignment, no secret to hold. A
  Netlify function has no managed identity; reaching Key Vault from Netlify
  means a client secret in a Netlify environment variable, which is a
  secret guarding secrets and exactly the pattern this design exists to end.
- **No 26-second cap.** Flex Consumption has no per-request ceiling. Netlify
  Pro functions die at 26 s, and a schema read of a large Elite 3E database
  does not fit in 26 s. Today's "azure mode" exists precisely to escape that
  cap; moving resolution to Netlify would reintroduce it for every connection.
- **The secret must not cross a third hop.** If Netlify resolved and then
  called Azure for the long-running part, the plaintext would travel between
  the two. Resolving where the work runs keeps it in one process.

Consequences for the Function App, all of which are work:

- `/api/db` gains `{ profileId, role, action, … }` and loses the ability to
  accept a connection string from the browser once migration ends.
- It gains **Postgres**. Today the Function App has `mssql` only
  (`azure-function/package.json`); `pg` and the dialect switch live in
  Netlify's `db-connect.js`. That code moves across.
- Authentication becomes **Entra token, strictly** (`authLevel: 'anonymous'`
  plus `verifyJwt` in code, as `conn-secrets.js` already does), with
  `REQUIRE_TOKEN_AUTH=true` and the host keys rotated. The function key
  stops being a thing the browser holds (§11).

**Netlify `db-connect.js`** keeps one job during migration — "paste a string
and test it" for an ad-hoc connection that is not yet in the register — and
is then retired. It cannot resolve a secret (no managed identity) and it
cannot run a long job (26 s), so nothing in the target state needs it.

## 5. Encryption: Key Vault through managed identity

Two ways to use Key Vault. Both are better than today's `CONN_SECRETS_KEY`
environment variable, which is a key that anyone with Portal access to the
Function App can read.

**A. One Key Vault *secret* per connection secret.** The Function App calls
`getSecret(secretRef)` at every use.

- Simple to explain and to audit in Key Vault's own logs.
- Every run is a Key Vault round trip (tens of ms, more on cold start), and
  Key Vault throttles at roughly 2,000 operations per 10 seconds per vault
  — a scheduled batch and a busy team hit that.
- Rotation is per secret, by hand. Listing scales badly. Each connection is a
  Key Vault object with its own lifecycle to keep in step with Cosmos.

**B. Envelope encryption: a Key Vault *key* wraps a per-secret data key; the
ciphertext lives in Cosmos.** The Function App calls `unwrapKey` once per use
(and may cache the unwrapped data key per instance for a few minutes), then
decrypts locally with AES-256-GCM.

- One Key Vault call per resolve, cacheable; Cosmos holds ciphertext only.
- Rotation is `rewrap` of the data keys, which is a script, not a migration.
- **Most of the code exists.** `conn-secrets.js` already does AES-256-GCM with
  a per-record IV, a GCM tag and an AAD binding the ciphertext to its owner
  and connection. The change is where the key comes from: a wrapped data key
  in the row, unwrapped by Key Vault, instead of one key in an environment
  variable. The AAD becomes `${tenantId}:${connId}`.
- Key Vault Keys are cheaper per operation than Secrets and are not the
  throttling concern at this scale.

**Recommendation: B.** Key Vault Secrets are kept for the two or three
org-level values the product itself needs (SMTP, Stripe), not for customer
connection secrets.

What this needs from Azure, none of it code: a Key Vault in UK South, a key
(`conn-kek`, RSA-3072 or AES-256 with wrap/unwrap), the Function App's managed
identity granted **Key Vault Crypto User** on it, and `@azure/keyvault-keys`
added to the Function App. I could not confirm from the repository whether a
vault exists — nothing imports a Key Vault SDK today — so this is an open
item, the same one CLAUDE.md lists as open question 2.

## 6. Locked connections

The existing rule stands: a connection referenced by a non-retired profile
cannot be edited in place (`cpIsConnLocked`), because editing would rewrite
the meaning of every artifact that ever ran under it.

- **Replacing the password is allowed** on a locked connection. It changes
  nothing about *which* database the profile points at; it is the rotation
  every security policy requires. It is audited (§8) and the register line
  updates its "updated … by …" fact.
- **Changing the endpoint** (server, database, port, auth type, user name) on
  a locked connection is refused. The path is: new connection, new profile
  that supersedes the old one, retire the old one — exactly as today.
- **Retiring a connection** is allowed only when no non-retired profile
  references it, mirroring profile retirement.

## 7. Permissions

The roles that exist, and what each may do with a connection. Grants use the
existing matrix vocabulary (`F` full, `L` limited, `R` read, `A` approve).

| Act | OW | PA | ML | EN | AP | DO | VA | AU | MB | SP |
|---|---|---|---|---|---|---|---|---|---|---|
| `connection.read` (metadata) | R | F | F | F | R | R | R | F | R | R |
| `connection.create` | – | F | – | – | – | – | – | – | – | – |
| `connection.replace-secret` | – | F | – | – | – | – | – | – | – | – |
| `connection.retire` | – | F | – | – | – | – | – | – | – | – |
| `connection.use` (run via a profile) | – | – | F | L | – | – | L | – | – | F |

Three deliberate lines.

- **The Platform Administrator holds the secrets and cannot use them.** That
  is already the PA's summary in `rbac.js` ("Grants access and holds secrets.
  Cannot use either.") and this design finally gives it teeth: PA creates and
  replaces, and has no `connection.use`.
- **The Engineer is `L`: never PROD.** `envClass` on the connection gates the
  run, using the classification rule that already exists. The Validator is
  `L` in the other direction: read-only actions only.
- **The Automation Principal uses without a person present.** A schedule runs
  under a profile, and the profile resolves the connection. That is the whole
  point of server-held secrets.

### Fitting the two authorisation systems

Roles are decided in **Netlify Blobs** (`cygenix-org`, `rbac-admin.js`), and
resolution happens on **Azure**, which cannot read Netlify Blobs. Two bridges
are possible and I recommend the second.

1. *Token app roles only.* Azure reads the `Cygenix.*` roles claim from the
   verified Entra token (`rbac.rolesFromClaims`). Works with no bridge, but
   ignores every role assigned on the Users & Roles page or by
   `scripts/grant-role.js`, which are stored assignments the token never
   carries.
2. *A signed grant.* The browser asks a Netlify function
   (`POST /.netlify/functions/run-grant { profileId, role, action }`), which
   evaluates `rbac.can(actor, 'connection.use', …)` against the real roles,
   the profile's environment and the connection's lock state, records the
   *intent* in the audit chain, and returns a short-lived token (five
   minutes, HMAC or a Key Vault-signed JWT) naming the actor, tenant,
   profile, role and permitted action. The browser passes it to Azure with
   the run request; Azure verifies the signature and honours it. Authorisation
   is decided where the roles live and enforced where the secret lives.

The Cosmos `users.role === 'admin'` gate (`requireAdmin`) stays what it is:
the Function App's own administrative door. It is not consulted for
connection use; the grant is.

## 8. Audit

Every act on a connection lands in the existing chain, with `source:'server'`
because a function observed it.

| Act | Where recorded | Action | Category |
|---|---|---|---|
| Create connection | Netlify, at the create endpoint | `connection.create` | `connections` |
| Replace secret | Netlify, at the secret endpoint | `connection.secret.replace` | `security` (always on) |
| Retire | Netlify | `connection.retire` | `connections` |
| Use — intent | Netlify, when the grant is issued | `connection.use` | `connections`, re-filed `prod` when `envClass` is PRD |
| Use — outcome | Azure, to the Cosmos `audit` container and the profile store's run records | `connection.use.result` | — |

Detail carries names and ids: connection id, standard name, profile id, role,
environment, action. Never a value. `connection.create` and `replace-secret`
exist today as `connection.create/edit/delete` in the client allow-list; the
server-side versions are not allow-listed because they are behind
`rbac.can`, as the chain already distinguishes.

The split between intent (chain) and outcome (Cosmos) is honest rather than
ideal: Azure cannot append to the chain. If that gap matters for evidence
packs, the Function App can post the outcome back to the Netlify audit
function with a service token; that is a later refinement, noted here so it
is not forgotten.

## 9. What the browser stops doing

Twenty files in `public/` build a Function App URL or a connection string for
a direct call (`tgtFnUrl`, `.tgtConn`, `impGetConn`, `isFn(...)`). Each moves
to one helper, `CygenixRun.db({ profileId, role, action, … })`, which obtains
the grant and calls Azure. The live pair (`cygenix_project_connections`)
survives as *which profile and which role each side is*, not as strings.

That is the largest single piece of this work and the one most likely to
surface a caller nobody remembered. It is also the moment the
`srcConnString`/`tgtFnKey` fields — and the strip-on-upload and fill-from-
store machinery added in Phase 4 — stop existing.

## 10. Migration

One-way, per user, once. The register is org-owned afterwards.

1. **Create the org register from what exists.** For every saved connection
   in every user's `cygenix_saved_connections`, create an org connection from
   its metadata. Deduplicate on `(kind, server, database, userName)`; when two
   users saved the same database under different names, keep both names as
   aliases and one record.
2. **Move the secrets up, once, by the person who holds them.** The next time
   a user with a local secret opens Connections, the page offers *"Move this
   password to the organisation"* for each connection whose org record has no
   secret yet. Accepting sends it to the write-only endpoint. A user with no
   secret sees the org fact instead ("Password saved · updated … by …").
3. **Remove local copies.** Once an org record has a secret, the browser
   deletes its own copy for that connection. After a dated cutover
   (four weeks), local secrets are ignored entirely and
   `cygenix_saved_conn_secrets`, `cygenix_conn_secrets_*` and the
   `sconn_live_*` mirrors are removed from the inventory.
4. **Rewrite the live pair.** Each user's `cygenix_project_connections`
   becomes `{ profileId, srcRole:'SRC', tgtRole:'TGT' }` derived from their
   selected profile; users with no profile are asked to pick one — the
   product already refuses writes without a selected profile once profiles
   are adopted.
5. **Retire `conn_secrets`.** The per-user container is emptied after the
   cutover and then deleted.

Nothing in this migration decrypts an old secret on the server: the browser
holds the plaintext and sends it to the new endpoint. The old
`CONN_SECRETS_KEY` scheme therefore never has to have worked for the
migration to work — which is fortunate, because it never has.

## 11. Removing the keys

- **The hardcoded `FUNC_CODE`** the brief names is already gone from
  `cygenix-cosmos-sync.js`; one dead reference to `CygenixSync.funcCode`
  remains in `cygenix-project-summary.js` and is removed in Phase A.
- **User-typed `?code=` keys** end with §4: the browser never calls `/api/db`
  with a key because it never calls it with an endpoint at all. The
  `blob-credential` hand-off (and the Phase 1 auto-fill that reuses it) is
  retired once the Drive relay authenticates with the Entra token, which is
  the same change as `/api/db`.
- **Function App host keys** are rotated the day `REQUIRE_TOKEN_AUTH=true` is
  set, closing CLAUDE.md's open question 1. Until both are done, anyone who
  copied a key from the old client can still call the Function App directly.
- **`CONN_SECRETS_KEY`** is deleted once `conn_secrets` is retired.

## 12. Phased plan

| Phase | Builds | Depends on | Risk |
|---|---|---|---|
| **A. Org register** | `org_connections` container; `connection.create/read/retire`; Connections page lists the org register beside (not instead of) the personal one; dead `funcCode` removed | nothing | Low. Additive. |
| **B. Secret store** | Key Vault + key + role assignment (Portal); `@azure/keyvault-keys`; `org_secrets`; write-only endpoint; "Password saved · updated … by …" and Replace; Test by `connId` | A; a vault | Medium. First Key Vault dependency; cold-start latency on `unwrapKey` (cache per instance). |
| **C. Grant bridge** | `run-grant` on Netlify; signature verification on Azure; `connection.use` in the matrix; intent/outcome audit | A, B | Medium. New trust boundary; must be tested against forged, expired and replayed grants. |
| **D. Server-side resolution** | `/api/db` by `{profileId, role}`; Postgres on the Function App; strict Entra auth on that route; `CygenixRun.db()`; the twenty callers migrated one page at a time behind a per-page switch | C | **High.** The most code, the most callers, the 26 s difference in behaviour, and the Function App learning a second dialect. Ship page by page. |
| **E. Migration and removal** | §10 steps 1–5; `blob-credential` retired; `REQUIRE_TOKEN_AUTH=true`; host keys rotated; `CONN_SECRETS_KEY` deleted | D complete | Medium. Irreversible once local copies are deleted; the cutover date and the "move this password" prompt are what make it safe. |

Each phase is deployable on its own and leaves the product working. Nothing
is removed until its replacement has carried real traffic.

## 13. Risks worth writing down

- **The Function App becomes the only path to a customer's database.** A
  Flex Consumption cold start (several seconds) is now in front of every
  query; today direct-string connections go through Netlify's warm functions.
  Mitigation: the Function App's always-ready instance count set to 1, and
  the Postgres pool warmed the way the SQL pool is.
- **Key Vault availability is now database availability.** A vault outage
  stops every run. Mitigation: a per-instance cache of unwrapped data keys
  with a bounded lifetime, so a short outage is survived and a long one is
  visible.
- **Twenty callers.** Any page missed keeps building a URL that no longer has
  a key. Mitigation: `tests/secret-exposure.test.js` grows a rule that no
  file in `public/` references `tgtFnUrl`, `srcFnUrl` or `?code=` once Phase D
  ends; until then, Phase 1's red box says which side is unkeyed.
- **The grant is a bearer token.** Five-minute lifetime, bound to actor,
  tenant, profile, role and action, single-use for write actions. Anything
  longer or broader is a key by another name.
- **Org-scoping meets per-user Cosmos.** Everything else in `projects` is
  keyed by email. Two containers keyed by `tenantId` are a second shape in the
  same database; the profile store's merge already handles multi-machine
  writes, but org connections need an `updatedAt` last-writer rule and a
  test for it.
- **Demo week.** None of this should be started while demo users are signing
  in. Phase A is additive and could go first; Phases B–E change what runs
  under every query and want a quiet week and a rollback plan.

## 14. Open questions for the owner

1. Does a Key Vault exist in UK South, and does the Function App have a
   system-assigned managed identity? (CLAUDE.md open question 2.)
2. Is Postgres on the Function App acceptable, or must Postgres stay on
   Netlify for some customers? It decides whether Phase D is one path or two.
3. Should the Approver role be able to *use* a connection to verify a change,
   or only approve it? The table above says only approve.
4. The four-week cutover for local secrets: too short, too long?
