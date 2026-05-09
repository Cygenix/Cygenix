// ─────────────────────────────────────────────────────────────────────────────
// github-proxy.js — Phase 1a: GitHub REST API proxy
// ─────────────────────────────────────────────────────────────────────────────
// Endpoint: POST /api/github/{action}?code=<FUNC_KEY>
//
// Why a server-side proxy (vs calling github.com from the browser directly):
//   1. CORS — github.com does NOT send Access-Control-Allow-Origin for browser
//      callers, so direct calls from cygenix.co.uk would fail. The Function
//      sits in our own domain and is allowed by our existing CORS headers.
//   2. Audit trail — every github call goes through ctx.log, so we can see
//      what got pushed, when, and by which user (without ever logging the PAT).
//   3. Future-proofing — when Phase 3 (bidirectional sync) needs server-side
//      webhook receivers, that infrastructure already lives here.
//
// PAT handling:
//   - The PAT is sent by the browser in the request body as `pat`.
//   - It is forwarded straight to GitHub as `Authorization: token <pat>`.
//   - It is NEVER logged. NEVER stored on the server. NEVER written to Cosmos.
//   - Per-user GitHub config (repo names, last push timestamps) IS stored in
//     the Cosmos `users` document under `github_config` — but only metadata,
//     never credentials.
//
// Actions:
//   whoami         — verify a PAT, return { login, name, scopes, orgs }
//   repo-create    — create a new private repo under the Cygenix org
//   repo-get       — read a repo (used to check existence, default branch,
//                     latest commit SHA before a push)
//   commit-files   — atomic multi-file commit using the Git Data API
//                     (blobs → tree → commit → ref update). Returns the new
//                     commit SHA on success, or 409 with the remote SHA if
//                     the expected parent SHA does not match.
//
// Body shape (all actions):
//   { pat: "ghp_xxx...", ...action-specific fields }
//
// The function key (?code=) is still required because authLevel: 'function'.
// That is the same gate as /api/db and /api/data — all GitHub calls require
// both the function key (to reach the endpoint at all) AND a valid PAT (to
// reach github.com).
// ─────────────────────────────────────────────────────────────────────────────

const { app } = require('@azure/functions');

// ── CORS (matches existing functions in this index.js) ───────────────────────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-user-id',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type':                 'application/json'
};

const ok  = (body)            => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const err = (code, msg, extra) => ({
  status: code,
  headers: CORS,
  body: JSON.stringify({ error: msg, ...(extra || {}) })
});

// ── Cosmos client (lazy singleton, key-based auth) ───────────────────────────
let _cosmos = null;
function getCosmosContainer(containerName) {
  if (!_cosmos) {
    const { CosmosClient } = require('@azure/cosmos');
    _cosmos = new CosmosClient({
      endpoint: process.env.COSMOS_ENDPOINT,
      key:      process.env.COSMOS_KEY
    });
  }
  return _cosmos
    .database(process.env.COSMOS_DATABASE || 'cygenix')
    .container(containerName);
}

// ── Helper: get userId from request header (matches index.js pattern) ────────
function getUserId(req) {
  return req.headers.get('x-user-id') || req.query.get('userId') || null;
}

// ── Org name (the GitHub organisation that owns all project repos) ──────────
// Hard-coded for now. If you ever need to point at a different org (e.g. for
// a separate test environment) it could move to an env var, but baking it in
// at this stage means there is no chance of accidentally pushing to the wrong
// org if env config drifts.
const GITHUB_ORG = 'Cygenix';

// ── Helper: call the GitHub REST API and return parsed JSON ─────────────────
// Centralises auth header, content-type, and error handling. Throws an Error
// whose .status is the HTTP code and .body is the parsed response, so the
// caller can map specific GitHub errors (404 repo not found, 422 validation,
// 409 conflict, 401 bad token) to the right HTTP response back to the browser.
async function gh(pat, method, path, body, ctx) {
  const url  = `https://api.github.com${path}`;
  const init = {
    method,
    headers: {
      'Authorization':        `token ${pat}`,
      'Accept':                'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      'User-Agent':            'Cygenix-Function/1.0'
    }
  };
  if (body !== undefined) {
    init.headers['Content-Type'] = 'application/json';
    init.body = JSON.stringify(body);
  }

  // Log method + path but NEVER the body (request body for blob creation
  // contains file contents which can be large; for repo creation it is fine
  // but the consistent rule is "log shape, not contents").
  if (ctx) ctx.log(`gh: ${method} ${path}`);

  let resp;
  try {
    resp = await fetch(url, init);
  } catch (netErr) {
    // Network-level failure (DNS, TLS, connection reset). Surface as 502 so
    // the browser can distinguish "GitHub said no" from "we couldn't reach
    // GitHub at all".
    const e = new Error(`Network error calling GitHub: ${netErr.message}`);
    e.status = 502;
    throw e;
  }

  // GitHub returns 204 with no body for some calls (rare for what we use).
  // Parse defensively.
  const text = await resp.text();
  let parsed = null;
  if (text) {
    try { parsed = JSON.parse(text); }
    catch { parsed = { raw: text.slice(0, 500) }; }
  }

  if (!resp.ok) {
    const e = new Error(parsed?.message || `GitHub ${resp.status}`);
    e.status = resp.status;
    e.body   = parsed;
    throw e;
  }

  return { data: parsed, headers: resp.headers };
}

// ─────────────────────────────────────────────────────────────────────────────
// ACTION HANDLERS
// ─────────────────────────────────────────────────────────────────────────────

// whoami — verify a PAT.
// Calls GET /user. The X-OAuth-Scopes response header lists the granted
// scopes; we surface them so the UI can warn if `repo` is missing. We also
// fetch the orgs the PAT can see to confirm Cygenix org access (a PAT with
// `repo` scope can still be blocked from a specific org via SSO enforcement,
// so checking `repo` scope alone is not sufficient).
async function actionWhoami(pat, ctx) {
  const userResp = await gh(pat, 'GET', '/user', undefined, ctx);
  const scopes   = (userResp.headers.get('x-oauth-scopes') || '')
                    .split(',').map(s => s.trim()).filter(Boolean);

  // Fetch orgs separately. If this fails (e.g. PAT lacks `read:org`) we still
  // want whoami to succeed — just report orgs as empty. The UI uses orgs only
  // to hint "looks like the Cygenix org isn't accessible" — it is not a hard
  // gate.
  let orgs = [];
  try {
    const orgsResp = await gh(pat, 'GET', '/user/orgs', undefined, ctx);
    orgs = (orgsResp.data || []).map(o => o.login);
  } catch (e) {
    if (ctx) ctx.log(`whoami: org list unavailable (${e.status}) — continuing`);
  }

  return {
    login:           userResp.data.login,
    name:            userResp.data.name || null,
    scopes,
    orgs,
    has_repo_scope:  scopes.includes('repo'),
    sees_cygenix_org: orgs.includes(GITHUB_ORG)
  };
}

// repo-create — create a private repo under the Cygenix org.
// If the repo already exists we DO NOT treat that as an error. We instead
// return the existing repo (matching the "re-export pushes to the existing
// repo rather than creating a new one" rule we agreed for Phase 1).
async function actionRepoCreate(pat, body, ctx) {
  const { name, description } = body;
  if (!name || typeof name !== 'string') {
    return err(400, 'name (string) is required');
  }

  // Check if it already exists first. Cheaper than relying on the 422 from
  // POST /orgs/{org}/repos and clearer to the caller — we explicitly tell
  // them "we reused the existing repo" via `existed: true`.
  try {
    const existing = await gh(pat, 'GET', `/repos/${GITHUB_ORG}/${name}`, undefined, ctx);
    return ok({
      existed:        true,
      full_name:      existing.data.full_name,
      default_branch: existing.data.default_branch,
      html_url:       existing.data.html_url,
      private:        existing.data.private
    });
  } catch (e) {
    // Not a 404 → real error (e.g. 401 bad token). Bubble up.
    if (e.status !== 404) {
      return err(e.status || 500, e.message, { github: e.body });
    }
    // 404 → fall through to create.
  }

  try {
    const created = await gh(pat, 'POST', `/orgs/${GITHUB_ORG}/repos`, {
      name,
      description: description || `Cygenix project export — ${name}`,
      private:     true,            // non-negotiable per Phase 1 design
      auto_init:   true,            // creates an initial commit so `main` exists
      has_issues:  false,
      has_wiki:    false,
      has_projects: false
    }, ctx);

    return ok({
      existed:        false,
      full_name:      created.data.full_name,
      default_branch: created.data.default_branch,
      html_url:       created.data.html_url,
      private:        created.data.private
    });
  } catch (e) {
    return err(e.status || 500, e.message, { github: e.body });
  }
}

// repo-get — read a repo. Used by the browser before push to fetch the
// current default branch + latest commit SHA (which becomes the parent SHA
// for the next commit).
async function actionRepoGet(pat, body, ctx) {
  const { name } = body;
  if (!name) return err(400, 'name (string) is required');

  try {
    const repo = await gh(pat, 'GET', `/repos/${GITHUB_ORG}/${name}`, undefined, ctx);
    const branch = repo.data.default_branch;

    // Get the SHA at the tip of the default branch.
    const ref = await gh(pat, 'GET',
      `/repos/${GITHUB_ORG}/${name}/git/ref/heads/${branch}`, undefined, ctx);

    return ok({
      full_name:        repo.data.full_name,
      default_branch:   branch,
      html_url:         repo.data.html_url,
      latest_commit_sha: ref.data.object.sha
    });
  } catch (e) {
    return err(e.status || 500, e.message, { github: e.body });
  }
}

// commit-files — atomic multi-file commit.
//
// Input body:
//   {
//     pat:               "ghp_...",
//     name:              "project-slug",
//     branch:            "main",          (optional, defaults to default_branch)
//     expectedParentSha: "abc123...",     (optional — if provided and doesn't
//                                           match the actual ref, return 409)
//     message:           "Export project ...",
//     files: {
//       "path/to/file1.yml": "string content",
//       "path/to/file2.sql": "string content",
//       ...
//     }
//   }
//
// Flow:
//   1. Read the ref → currentSha
//   2. If expectedParentSha provided and != currentSha → 409 conflict
//   3. Read the commit at currentSha → baseTreeSha
//   4. Create a blob for each file (parallel)
//   5. Create a tree extending baseTreeSha with the new blobs
//   6. Create a commit pointing at the tree, parent = currentSha
//   7. Update the ref to point at the new commit
//
// Files are sent as UTF-8 strings. GitHub's blob API accepts base64; we
// always send base64 to avoid encoding edge cases (BOM, non-ASCII in SQL
// comments, etc.).
async function actionCommitFiles(pat, body, ctx) {
  const { name, message, files } = body;
  let { branch, expectedParentSha } = body;

  if (!name)    return err(400, 'name (string) is required');
  if (!message) return err(400, 'message (string) is required');
  if (!files || typeof files !== 'object') {
    return err(400, 'files (object of path → content) is required');
  }
  const paths = Object.keys(files);
  if (paths.length === 0) {
    return err(400, 'files object must contain at least one entry');
  }

  // Sanity-cap. If you ever push more than 1000 files in one commit something
  // is probably wrong upstream. GitHub itself accepts much more, but failing
  // loudly here is safer than silently DoSing the API.
  if (paths.length > 1000) {
    return err(400, `too many files in one commit: ${paths.length} > 1000`);
  }

  try {
    // Resolve branch if not given.
    if (!branch) {
      const repo = await gh(pat, 'GET', `/repos/${GITHUB_ORG}/${name}`, undefined, ctx);
      branch = repo.data.default_branch;
    }

    // Step 1: current ref.
    const ref = await gh(pat, 'GET',
      `/repos/${GITHUB_ORG}/${name}/git/ref/heads/${branch}`, undefined, ctx);
    const currentSha = ref.data.object.sha;

    // Step 2: conflict check.
    if (expectedParentSha && expectedParentSha !== currentSha) {
      return {
        status:  409,
        headers: CORS,
        body: JSON.stringify({
          error:             'Remote has diverged from the expected parent commit',
          expectedParentSha,
          actualParentSha:   currentSha,
          branch
        })
      };
    }

    // Step 3: base tree.
    const baseCommit = await gh(pat, 'GET',
      `/repos/${GITHUB_ORG}/${name}/git/commits/${currentSha}`, undefined, ctx);
    const baseTreeSha = baseCommit.data.tree.sha;

    // Step 4: create blobs in parallel. Each blob upload is one HTTP call;
    // for projects with hundreds of files this is the slowest stage. Done
    // in parallel via Promise.all — GitHub's per-IP rate limit (5000/hr for
    // authenticated calls) is well above what any reasonable push will
    // need, and we're bounded by 1000 files above.
    const blobResults = await Promise.all(paths.map(async (p) => {
      const content = files[p];
      const b64 = Buffer.from(
        typeof content === 'string' ? content : String(content),
        'utf8'
      ).toString('base64');
      const blob = await gh(pat, 'POST',
        `/repos/${GITHUB_ORG}/${name}/git/blobs`,
        { content: b64, encoding: 'base64' },
        ctx
      );
      return { path: p, sha: blob.data.sha };
    }));

    // Step 5: tree.
    const tree = await gh(pat, 'POST',
      `/repos/${GITHUB_ORG}/${name}/git/trees`,
      {
        base_tree: baseTreeSha,
        tree: blobResults.map(b => ({
          path: b.path,
          mode: '100644',          // regular file
          type: 'blob',
          sha:  b.sha
        }))
      },
      ctx
    );

    // Step 6: commit.
    const commit = await gh(pat, 'POST',
      `/repos/${GITHUB_ORG}/${name}/git/commits`,
      {
        message,
        tree:    tree.data.sha,
        parents: [currentSha]
      },
      ctx
    );

    // Step 7: update ref.
    await gh(pat, 'PATCH',
      `/repos/${GITHUB_ORG}/${name}/git/refs/heads/${branch}`,
      { sha: commit.data.sha, force: false },
      ctx
    );

    return ok({
      commit_sha:      commit.data.sha,
      commit_html_url: commit.data.html_url
                        || `https://github.com/${GITHUB_ORG}/${name}/commit/${commit.data.sha}`,
      branch,
      file_count:      paths.length
    });
  } catch (e) {
    // If the ref update fails with 422, it's almost always because someone
    // pushed between our ref-read and our ref-patch. Surface as 409 so the
    // UI shows the conflict path, not a generic error.
    if (e.status === 422 && /update is not a fast forward/i.test(e.message || '')) {
      return {
        status: 409,
        headers: CORS,
        body: JSON.stringify({
          error:  'Remote moved during push (fast-forward failed)',
          github: e.body
        })
      };
    }
    return err(e.status || 500, e.message, { github: e.body });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// ROUTE: /api/github/{action}
// ─────────────────────────────────────────────────────────────────────────────
app.http('github', {
  methods:   ['POST', 'OPTIONS'],
  authLevel: 'function',
  route:     'github/{action}',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 200, headers: CORS, body: '' };

    const action = req.params.action;
    const userId = getUserId(req);
    if (!userId) return err(401, 'x-user-id header is required');

    ctx.log(`github/${action} for user: ${userId}`);

    let body = {};
    try { body = await req.json(); } catch {}

    const pat = body.pat;
    if (!pat || typeof pat !== 'string' || pat.length < 20) {
      return err(400, 'pat (GitHub Personal Access Token) is required in body');
    }

    try {
      switch (action) {
        case 'whoami':
          return ok(await actionWhoami(pat, ctx));

        case 'repo-create':
          return await actionRepoCreate(pat, body, ctx);

        case 'repo-get':
          return await actionRepoGet(pat, body, ctx);

        case 'commit-files':
          return await actionCommitFiles(pat, body, ctx);

        default:
          return err(404, `Unknown action: ${action}`);
      }
    } catch (e) {
      // Catch-all. Anything thrown by an action handler ends up here. We
      // include the message and a short stack so debugging via the response
      // body is possible (Application Insights / Log Stream are unavailable
      // on the current Function plan — this is the in-band debug pattern
      // already used elsewhere in this codebase).
      ctx.log(`github/${action} error: ${e.message}`);
      return err(e.status || 500, e.message, {
        stack: String(e.stack || '').split('\n').slice(0, 6).join(' | ')
      });
    }
  }
});
