/**
 * admin-auth.js
 *
 * Centralised admin role-gating for the Cygenix Azure Function.
 *
 * Two exports:
 *   - whoamiHandler(context, req, cosmos)   → Function handler for GET /api/data/whoami
 *   - requireAdmin(context, req, cosmos)    → Middleware. Returns true if caller is admin,
 *                                             OR sets a 403 response and returns false.
 *
 * Source of truth: the `role` field on the user's document in the Cosmos
 * `users` container. Lookup key is the email address from the `x-user-id`
 * header (lowercased), matching how cygenix-cosmos-sync.js writes it.
 *
 * Why a header and not a JWT signature check? The Azure Function already
 * trusts the `x-user-id` header for every other endpoint, so introducing
 * JWT verification here would be inconsistent. Once the Stripe wiring lands
 * we'll likely want to harden the whole API with proper token verification
 * — for now, role is checked via the same header path as everything else.
 */

// `cosmos` is the same Cosmos client object the rest of your Function uses.
// If your Function uses a different name (e.g. `db`, `client`, `users`), just
// rename the param at the call site — these helpers don't care what it's
// called as long as they can do `.container('users').item(id, id).read()`.

async function readUserRole(cosmos, email) {
  if (!email) return null;
  const id = email.trim().toLowerCase();
  try {
    const { resource } = await cosmos.container('users').item(id, id).read();
    return resource?.role || null;
  } catch (e) {
    // 404 = user doc doesn't exist (first sign-in hasn't happened yet, or
    // the email doesn't match anyone). Either way, not admin.
    if (e.code === 404) return null;
    // Anything else is a real error — re-throw so the outer try/catch
    // surfaces it in the 500 response (per Cygenix in-band debugging convention).
    throw e;
  }
}

/**
 * GET /api/data/whoami
 *
 * Returns the caller's role so the frontend role-gate in admin.html can
 * decide whether to render the page. Does NOT return any other user info —
 * keep this endpoint narrow.
 *
 * Response shape:
 *   200 { email, role }   role may be 'admin', 'user', or null
 *   401 { error: 'no-user-id' }
 */
async function whoamiHandler(context, req, cosmos) {
  const email = (req.headers['x-user-id'] || '').trim().toLowerCase();
  if (!email) {
    context.res = { status: 401, body: { error: 'no-user-id' } };
    return;
  }
  try {
    const role = await readUserRole(cosmos, email);
    context.res = { status: 200, body: { email, role } };
  } catch (e) {
    // In-band debugging: surface the error in the response body, since
    // App Insights / Live Stream / Kudu aren't available on this Azure plan.
    context.res = {
      status: 500,
      body: { error: 'whoami-failed', message: e.message, stack: e.stack }
    };
  }
}

/**
 * requireAdmin(context, req, cosmos)
 *
 * Drop this at the top of any admin-only handler:
 *
 *     module.exports = async function (context, req) {
 *       if (!(await requireAdmin(context, req, cosmos))) return;
 *       // ... rest of handler ...
 *     };
 *
 * Returns true on success (caller continues). Returns false AND sets
 * context.res to a 401/403 response on failure (caller MUST return immediately).
 */
async function requireAdmin(context, req, cosmos) {
  const email = (req.headers['x-user-id'] || '').trim().toLowerCase();
  if (!email) {
    context.res = { status: 401, body: { error: 'no-user-id' } };
    return false;
  }
  let role;
  try {
    role = await readUserRole(cosmos, email);
  } catch (e) {
    context.res = {
      status: 500,
      body: { error: 'role-check-failed', message: e.message, stack: e.stack }
    };
    return false;
  }
  if (role !== 'admin') {
    context.res = {
      status: 403,
      body: { error: 'admin-required', email, role: role || null }
    };
    return false;
  }
  return true;
}

module.exports = { whoamiHandler, requireAdmin };
