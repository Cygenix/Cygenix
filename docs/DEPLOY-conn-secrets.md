# Deploy note — the encrypted credential store needs a key

**Status: outstanding until somebody sets `CONN_SECRETS_KEY` on the Function App.**

## What is wrong

Saved-connection credentials (passwords, connection strings, Function App
keys) sync to Cosmos encrypted, through `azure-function/src/conn-secrets.js`.
That module seals every credential with a key it reads from the environment
variable `CONN_SECRETS_KEY`. It reads the key per request and, if the key is
missing or not a 32-byte base64 value, answers **503**:

```
{"error":"secrets store not configured","code":"no-secrets-key"}
```

That is the 64-byte response the browser sees on `data-proxy?action=secrets-list`.

The key has never been set. So since the feature shipped (commit `613f819`):

- every `secrets-list` has answered 503, so no device has ever pulled a credential;
- every `secrets-put` has answered 503, so **no credential has ever been persisted for any user**;
- after a cookie clear, every saved connection comes back without its password
  and every azure-mode connection comes back without its key, and the Function
  App's `/api/db` answers 401.

Since Sep-2026 the Connections page and the Profiles page say this in a red box
instead of a console warning.

## Fix (Azure Portal, about two minutes)

1. Generate a key. Any machine with Node:

   ```bash
   node -e "console.log(require('crypto').randomBytes(32).toString('base64'))"
   ```

   It must decode to exactly 32 bytes; the command above guarantees that.

2. Azure Portal → Function App **cygenix-db-api** → Settings → **Environment variables**
   → Add:

   | Name | Value |
   |---|---|
   | `CONN_SECRETS_KEY` | the value from step 1 |
   | `CONN_SECRETS_KEY_VERSION` | `1` (optional; defaults to 1) |

3. Save. The Function App restarts. No code deploy is needed: the key is read
   per request.

4. Verify. Sign in to Cygenix, open **Connections**. The red "Credentials are
   not syncing" box should be gone. In the browser's Network tab,
   `data-proxy?action=secrets-list` should return 200 with a JSON body that has
   `secrets` and `undecryptable` keys.

5. Each user then re-enters their passwords once (the chips say "Password
   needed on this device"). From then on the credential follows them to every
   device.

## Rotating the key later

Change `CONN_SECRETS_KEY` and bump `CONN_SECRETS_KEY_VERSION`. Records sealed
under the old key are reported as `undecryptable` and the page asks for them
to be re-entered; nothing is lost silently and nothing is decryptable by the
old key holder.

## Keep the key out of

- the repository, in any file;
- Netlify environment variables (only the Function App needs it);
- chat, tickets and screenshots.
