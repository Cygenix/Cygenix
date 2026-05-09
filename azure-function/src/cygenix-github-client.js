/* ─────────────────────────────────────────────────────────────────────────────
 * cygenix-github-client.js — Phase 1a: Browser-side GitHub client
 * ─────────────────────────────────────────────────────────────────────────────
 * What this file is:
 *   - The thin browser layer that talks to /api/github/{action}.
 *   - PAT storage in localStorage (per-user, browser-local, never synced).
 *   - Repo + push orchestration helpers.
 *   - NO knowledge of Cygenix project structure. That belongs to the Phase 1b
 *     project-exporter module which will produce the `files` map this client
 *     accepts.
 *   - NO UI code. Phase 1c builds the settings panel; Phase 1d builds the
 *     push button. Both will call into this client.
 *
 * Public API (attached to window.CygenixGitHub):
 *   getPat()                     → string | null
 *   setPat(token)                → void
 *   clearPat()                   → void
 *   isConfigured()               → boolean
 *   whoami()                     → Promise<{ login, scopes, has_repo_scope, ... }>
 *   ensureRepo(name, opts)       → Promise<{ existed, full_name, default_branch, html_url }>
 *   getRepoState(name)           → Promise<{ default_branch, latest_commit_sha, html_url }>
 *   pushFiles(name, opts)        → Promise<{ commit_sha, commit_html_url, file_count }>
 *
 * Function key:
 *   The Azure Function uses authLevel: 'function', so every call needs ?code=.
 *   We re-use whatever the rest of the dashboard already uses to call /api/db
 *   and /api/data. The function key is read from window.CYGENIX_FUNCTION_KEY,
 *   set elsewhere at page-load. If a different mechanism is in place (e.g.
 *   the URL is built by a helper) you can override the FUNCTION_BASE constant
 *   in one place at the top.
 *
 * User identity:
 *   The endpoint requires the x-user-id header (matches /api/data, /api/agent
 *   etc.). We read window.CYGENIX_USER_ID, which is set by the existing auth
 *   layer after MSAL sign-in. If it's missing, calls fail loudly with a clear
 *   error rather than silently dropping the header.
 * ─────────────────────────────────────────────────────────────────────────────
 */
(function () {
  'use strict';

  // ── Config — adjust here if the rest of the codebase moves ────────────────
  // FUNCTION_BASE: full base URL of the Azure Function. The same value the
  // existing /api/db calls use. If your dashboard already centralises this in
  // a constant or a helper (e.g. window.CYGENIX_FN_BASE), change the line
  // below to read from there instead. Hard-coded to the production endpoint
  // for now to match how /api/db is currently invoked from the page.
  const FUNCTION_BASE =
    (typeof window !== 'undefined' && window.CYGENIX_FN_BASE) ||
    'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net';

  // localStorage keys. Prefixed `cygenix_gh_` so they're easy to spot and
  // wipe in devtools, and so they don't collide with anything else (the
  // existing app uses cygenix_* without the gh_ infix).
  const LS_PAT     = 'cygenix_gh_pat';
  const LS_LOGIN   = 'cygenix_gh_login';   // remembered after a successful whoami,
                                            // for showing "Connected as @username"
                                            // in the UI without re-calling whoami

  // ── Internal: build a function-key-aware URL ──────────────────────────────
  function fnUrl(action) {
    // Read the function key fresh each call. If it's rotated mid-session,
    // re-loading the page will pick up the new one without a code change.
    const key = (typeof window !== 'undefined' && window.CYGENIX_FUNCTION_KEY) || '';
    const sep = key ? `?code=${encodeURIComponent(key)}` : '';
    return `${FUNCTION_BASE}/api/github/${action}${sep}`;
  }

  // ── Internal: get current user id, throwing if missing ────────────────────
  function requireUserId() {
    const uid = (typeof window !== 'undefined' && window.CYGENIX_USER_ID) || '';
    if (!uid) {
      throw new Error(
        'CygenixGitHub: window.CYGENIX_USER_ID is not set. The user must be ' +
        'signed in before calling GitHub features.'
      );
    }
    return uid;
  }

  // ── Internal: POST to a GitHub-proxy action ───────────────────────────────
  // Always includes the PAT and userId. Throws Error with .status and .body
  // populated on non-2xx so callers can branch on error type (409 conflict
  // is the main one that needs special handling in the UI).
  async function call(action, payload) {
    const pat = getPat();
    if (!pat) {
      const e = new Error('No GitHub PAT configured. Open Develop → GitHub to set one.');
      e.code = 'NO_PAT';
      throw e;
    }

    const resp = await fetch(fnUrl(action), {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-user-id':    requireUserId()
      },
      body: JSON.stringify({ pat, ...(payload || {}) })
    });

    let data = null;
    const text = await resp.text();
    if (text) {
      try { data = JSON.parse(text); } catch { data = { raw: text.slice(0, 500) }; }
    }

    if (!resp.ok) {
      const e = new Error((data && data.error) || `github/${action} failed: ${resp.status}`);
      e.status = resp.status;
      e.body   = data;
      throw e;
    }

    return data;
  }

  // ── PAT storage ───────────────────────────────────────────────────────────
  function getPat() {
    try { return localStorage.getItem(LS_PAT) || null; }
    catch { return null; }
  }

  function setPat(token) {
    if (!token || typeof token !== 'string') {
      throw new Error('setPat: token must be a non-empty string');
    }
    // Trim — paste from github.com sometimes drags whitespace.
    const trimmed = token.trim();
    localStorage.setItem(LS_PAT, trimmed);
  }

  function clearPat() {
    localStorage.removeItem(LS_PAT);
    localStorage.removeItem(LS_LOGIN);
  }

  function isConfigured() {
    return !!getPat();
  }

  function getRememberedLogin() {
    try { return localStorage.getItem(LS_LOGIN) || null; }
    catch { return null; }
  }

  // ── Public actions ────────────────────────────────────────────────────────

  /**
   * Verify the stored PAT. Caches the login on success so the UI can show
   * "Connected as @login" without re-hitting GitHub on every page load.
   * Throws on bad token, network error, or missing PAT.
   */
  async function whoami() {
    const data = await call('whoami', {});
    try { localStorage.setItem(LS_LOGIN, data.login || ''); } catch {}
    return data;
  }

  /**
   * Idempotently get-or-create a private repo under the Cygenix org.
   * Returns { existed, full_name, default_branch, html_url, private }.
   *
   * `name` should already be a valid GitHub repo name (slug-cased,
   * no spaces). The Phase 1b exporter is responsible for slugging
   * project names — this client does not transform `name`.
   */
  async function ensureRepo(name, opts) {
    return await call('repo-create', {
      name,
      description: (opts && opts.description) || undefined
    });
  }

  /**
   * Read repo state. Used by callers to fetch `latest_commit_sha` before a
   * push, which is then passed back as `expectedParentSha` to detect
   * concurrent edits on the GitHub side.
   */
  async function getRepoState(name) {
    return await call('repo-get', { name });
  }

  /**
   * Push a set of files as a single atomic commit.
   *
   * @param {string} name - repo name (under Cygenix org)
   * @param {object} opts
   *   - files: { "path/to/file.yml": "string content", ... }
   *   - message: commit message
   *   - branch: optional, defaults to repo's default_branch
   *   - expectedParentSha: optional, for conflict detection. If provided
   *       and remote has moved, throws an Error with .status === 409 and
   *       .body containing { actualParentSha }.
   *
   * Returns { commit_sha, commit_html_url, branch, file_count }.
   */
  async function pushFiles(name, opts) {
    if (!opts || !opts.files || !opts.message) {
      throw new Error('pushFiles: opts.files and opts.message are required');
    }
    return await call('commit-files', {
      name,
      branch:            opts.branch,
      expectedParentSha: opts.expectedParentSha,
      message:           opts.message,
      files:             opts.files
    });
  }

  // ── Convenience: full "push project" orchestration ────────────────────────
  // High-level helper that bundles ensureRepo → getRepoState → pushFiles
  // into one call. The Phase 1d push button will use this.
  //
  // Returns:
  //   { ok: true, repo, commit }                          on success
  //   { ok: false, conflict: true, repo, actualParentSha } on conflict
  //   throws                                              on hard errors
  //
  // `force: true` re-runs the push without a parent-SHA check, intended for
  // the "Overwrite" button on the conflict warning. This is the equivalent
  // of `git push --force` and the UI must surface that clearly.
  async function pushProject(opts) {
    if (!opts || !opts.repoName || !opts.files || !opts.message) {
      throw new Error('pushProject: repoName, files, and message are required');
    }

    // 1. Ensure the repo exists.
    const repo = await ensureRepo(opts.repoName, {
      description: opts.description
    });

    // 2. Read current state to get the parent SHA, unless forcing.
    let expectedParentSha;
    if (!opts.force) {
      const state = await getRepoState(opts.repoName);
      expectedParentSha = state.latest_commit_sha;
    }

    // 3. Push.
    try {
      const commit = await pushFiles(opts.repoName, {
        files:   opts.files,
        message: opts.message,
        branch:  repo.default_branch,
        expectedParentSha
      });
      return { ok: true, repo, commit };
    } catch (e) {
      if (e.status === 409 && e.body) {
        return {
          ok:               false,
          conflict:         true,
          repo,
          actualParentSha:  e.body.actualParentSha || null,
          message:          e.message
        };
      }
      throw e;
    }
  }

  // ── Expose ────────────────────────────────────────────────────────────────
  window.CygenixGitHub = {
    // PAT management
    getPat,
    setPat,
    clearPat,
    isConfigured,
    getRememberedLogin,
    // Direct API
    whoami,
    ensureRepo,
    getRepoState,
    pushFiles,
    // High-level
    pushProject
  };
})();
