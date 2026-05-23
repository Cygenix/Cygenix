// project-memory.js
//
// GET    /api/agent/project-memory?projectId=<id>&code=<FUNC_KEY>
// POST   /api/agent/project-memory?code=<FUNC_KEY>
// DELETE /api/agent/project-memory?code=<FUNC_KEY>
//
// Per-user, per-project memory store for the Agentive Migration agent.
// Persists facts, decisions, preferences and warnings the agent learns
// during a run, so the next run starts with context instead of from
// scratch.
//
// Container: `project_memory` (Cosmos)
// Partition key field: `userProjectKey` = "<userId>::<projectId>"
//
// Spec: docs/project-memory-schema.md
// Tier rules: docs/project-memory-tiering-rules.md
//
// Auth: `x-user-id` request header is the source of truth for userId.
// Client-supplied userId on documents is ignored, same pattern as
// `profile-save` in index.js.
//
// Tiering: see tiering-rules doc. Briefly:
//   - low-stakes (kind=fact|decision): auto-written, confirmedBy='auto'
//   - high-stakes (kind=preference|warning): require confirmedBy='user'
//     in the request, otherwise rejected with 400. The agent holds
//     unconfirmed proposals in its own run state, not here.

const { app } = require('@azure/functions');

// ── CORS (matches existing modules in this project) ──────────────────────────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-user-id',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Content-Type':                 'application/json'
};

const ok  = (body)            => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const err = (code, msg, stack) => ({
  status: code,
  headers: CORS,
  body: JSON.stringify({ error: msg, ...(stack ? { stack } : {}) })
});

// ── Cosmos client (lazy singleton, same pattern as index.js) ─────────────────
let _cosmos = null;
function getContainer() {
  if (!_cosmos) {
    const { CosmosClient } = require('@azure/cosmos');
    _cosmos = new CosmosClient({
      endpoint: process.env.COSMOS_ENDPOINT,
      key:      process.env.COSMOS_KEY
    });
  }
  return _cosmos
    .database(process.env.COSMOS_DATABASE || 'cygenix')
    .container('project_memory');
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function getUserId(req) {
  return req.headers.get('x-user-id') || req.query.get('userId') || null;
}

function getProjectId(req, body) {
  // Accept from query (GET/DELETE) or body (POST), depending on method.
  return (
    req.query.get('projectId') ||
    (body && body.projectId) ||
    null
  );
}

function makeUserProjectKey(userId, projectId) {
  return `${userId}::${projectId}`;
}

// Short random id for new entries. Cosmos doc id is composed as
// `${userId}_${projectId}_${entryId}` to mirror the data_profiles
// convention and make manual debugging in the Cosmos data explorer easy.
function newEntryId() {
  const crypto = require('crypto');
  return crypto.randomBytes(8).toString('hex');
}

// ── Validation ───────────────────────────────────────────────────────────────

const VALID_KINDS    = new Set(['fact', 'preference', 'decision', 'warning']);
const VALID_STATUSES = new Set(['active', 'superseded', 'rejected']);
const LOW_STAKES_KINDS  = new Set(['fact', 'decision']);
const HIGH_STAKES_KINDS = new Set(['preference', 'warning']);

// Map kind → expected stakes (server-derived, never trusted from client).
// Tiering-rules.md is the source of truth; this enforces it in code.
function stakesFor(kind) {
  if (LOW_STAKES_KINDS.has(kind))  return 'low';
  if (HIGH_STAKES_KINDS.has(kind)) return 'high';
  return null;
}

// Validate a proposed entry from the request body. Returns { ok: true, entry }
// on success, or { ok: false, code, msg } on failure.
function validateEntry(input) {
  if (!input || typeof input !== 'object') {
    return { ok: false, code: 400, msg: 'entry object is required' };
  }
  if (!input.kind || !VALID_KINDS.has(input.kind)) {
    return { ok: false, code: 400, msg: `kind must be one of: ${[...VALID_KINDS].join(', ')}` };
  }
  if (!input.subject || typeof input.subject !== 'string' || input.subject.length > 200) {
    return { ok: false, code: 400, msg: 'subject is required (non-empty string, max 200 chars)' };
  }
  if (!input.content || typeof input.content !== 'string') {
    return { ok: false, code: 400, msg: 'content is required (non-empty string)' };
  }
  if (input.content.length > 8000) {
    return { ok: false, code: 400, msg: 'content exceeds 8000 chars — split into multiple entries' };
  }
  return { ok: true };
}

// ── Read: GET /agent/project-memory ──────────────────────────────────────────
// Returns every active memory entry for caller+project. Optional ?status=
// filter to retrieve superseded or rejected entries for audit views.
async function handleGet(req, ctx) {
  const userId    = getUserId(req);
  const projectId = getProjectId(req, null);
  if (!userId)    return err(401, 'x-user-id header is required');
  if (!projectId) return err(400, 'projectId query param is required');

  const key       = makeUserProjectKey(userId, projectId);
  const statusFilter = req.query.get('status') || 'active';

  if (!VALID_STATUSES.has(statusFilter) && statusFilter !== 'all') {
    return err(400, `status must be one of: ${[...VALID_STATUSES].join(', ')}, or 'all'`);
  }

  try {
    const querySpec = statusFilter === 'all'
      ? {
          query: 'SELECT * FROM c WHERE c.userProjectKey = @key ORDER BY c.updatedAt DESC',
          parameters: [{ name: '@key', value: key }]
        }
      : {
          query: 'SELECT * FROM c WHERE c.userProjectKey = @key AND c.status = @status ORDER BY c.updatedAt DESC',
          parameters: [
            { name: '@key',    value: key          },
            { name: '@status', value: statusFilter }
          ]
        };

    const { resources } = await getContainer().items
      .query(querySpec, { partitionKey: key })
      .fetchAll();

    return ok({
      userId,
      projectId,
      count:   (resources || []).length,
      entries: resources || []
    });
  } catch (e) {
    ctx.log(`[project-memory] GET failed: ${e.message}`);
    return err(500, e.message, (e.stack || '').split('\n').slice(0, 6).join('\n'));
  }
}

// ── Write: POST /agent/project-memory ────────────────────────────────────────
//
// Body shape:
//   {
//     "projectId": "<id>",
//     "entry": {
//       "kind":    "fact" | "preference" | "decision" | "warning",
//       "subject": "<short identifier>",
//       "content": "<the memorable thing>",
//       "evidence": { "source": "agent-observation" | "user-statement" | "run-result",
//                     "runId": "...", "query": "...", "userMessage": "..." },
//       "confirmedBy": "auto" | "user"
//     }
//   }
//
// Tiering rule enforced server-side:
//   - kind in {fact, decision}      → stakes='low',  confirmedBy='auto' allowed
//   - kind in {preference, warning} → stakes='high', confirmedBy MUST be 'user'
//
// Supersede behaviour: if an active entry already exists for the same subject,
// it is updated to status='superseded' and the new entry takes its place. Two
// separate Cosmos ops, not a transaction — see schema doc §8.3.
async function handlePost(req, ctx) {
  const userId = getUserId(req);
  if (!userId) return err(401, 'x-user-id header is required');

  let body;
  try { body = await req.json(); }
  catch { return err(400, 'request body must be valid JSON'); }

  const projectId = getProjectId(req, body);
  if (!projectId) return err(400, 'projectId is required (in body or query)');

  const input = body && body.entry;
  const v = validateEntry(input);
  if (!v.ok) return err(v.code, v.msg);

  // Server-derive stakes from kind. NEVER trust client-supplied stakes.
  const stakes = stakesFor(input.kind);
  if (!stakes) return err(400, `internal: unknown kind ${input.kind}`); // unreachable after validateEntry

  // Tiering gate: high-stakes entries require explicit user confirmation.
  // Low-stakes default to auto when confirmedBy is absent.
  let confirmedBy = input.confirmedBy;
  if (stakes === 'high') {
    if (confirmedBy !== 'user') {
      return err(400,
        `High-stakes entries (kind=${input.kind}) require confirmedBy='user'. ` +
        `Proposals must be confirmed by the user before writing to memory. ` +
        `See docs/project-memory-tiering-rules.md.`);
    }
  } else {
    if (!confirmedBy) confirmedBy = 'auto';
    if (confirmedBy !== 'auto' && confirmedBy !== 'user') {
      return err(400, `confirmedBy must be 'auto' or 'user'`);
    }
  }

  const now    = new Date().toISOString();
  const key    = makeUserProjectKey(userId, projectId);
  const entryId = newEntryId();
  const container = getContainer();

  // Look for an existing active entry on the same subject — to supersede.
  let existing = null;
  try {
    const { resources } = await container.items
      .query({
        query: `SELECT TOP 1 * FROM c
                WHERE c.userProjectKey = @key
                  AND c.subject = @subject
                  AND c.status  = 'active'
                ORDER BY c.createdAt DESC`,
        parameters: [
          { name: '@key',     value: key           },
          { name: '@subject', value: input.subject }
        ]
      }, { partitionKey: key })
      .fetchAll();
    existing = (resources && resources[0]) || null;
  } catch (e) {
    ctx.log(`[project-memory] supersede lookup failed (non-fatal, proceeding): ${e.message}`);
    // Non-fatal — worst case we end up with two active entries for the same
    // subject; the read path selects TOP 1 by createdAt DESC so this is
    // self-healing on the next write.
  }

  // Build the new entry doc. Authoritative fields are set server-side.
  const newDoc = {
    id:             `${userId}_${projectId}_${entryId}`,
    userProjectKey: key,
    userId,
    projectId,
    entryId,
    kind:    input.kind,
    stakes,
    status:  'active',
    subject: input.subject,
    content: input.content,
    evidence: input.evidence && typeof input.evidence === 'object' ? input.evidence : { source: 'agent-observation' },
    supersedes:   existing ? existing.entryId : undefined,
    supersededBy: undefined,
    createdAt:   now,
    updatedAt:   now,
    confirmedAt: now,
    confirmedBy
  };

  try {
    // Write new entry first, then mark old one superseded. Order chosen so
    // that if step 2 fails, the user sees the new entry (correct) plus an
    // orphan old "active" entry — the supersede read query takes TOP 1
    // by createdAt DESC, so the newer one wins regardless.
    await container.items.upsert(newDoc);

    if (existing) {
      try {
        const updated = {
          ...existing,
          status:       'superseded',
          supersededBy: entryId,
          updatedAt:    now
        };
        await container.items.upsert(updated);
      } catch (e) {
        ctx.log(`[project-memory] supersede update failed for ${existing.id}: ${e.message}`);
        // Non-fatal; see comment above. New entry is already saved.
      }
    }

    return ok({
      saved:        true,
      entry:        newDoc,
      supersededId: existing ? existing.entryId : null
    });
  } catch (e) {
    ctx.log(`[project-memory] POST upsert failed: ${e.message}`);
    return err(500, e.message, (e.stack || '').split('\n').slice(0, 6).join('\n'));
  }
}

// ── Delete: DELETE /agent/project-memory ─────────────────────────────────────
//
// Two modes:
//   - DELETE ?projectId=X&entryId=Y  → delete one entry (hard delete)
//   - DELETE ?projectId=X&all=true   → wipe all entries for caller+project
//
// User edits in the UI translate to "delete + new POST" rather than mutating
// in place. This keeps the audit trail in the supersede chain rather than
// hidden inside entry version history.
async function handleDelete(req, ctx) {
  const userId    = getUserId(req);
  const projectId = getProjectId(req, null);
  if (!userId)    return err(401, 'x-user-id header is required');
  if (!projectId) return err(400, 'projectId query param is required');

  const entryId = req.query.get('entryId');
  const wipeAll = req.query.get('all') === 'true';

  if (!entryId && !wipeAll) {
    return err(400, 'either entryId or all=true must be specified');
  }
  if (entryId && wipeAll) {
    return err(400, 'specify entryId OR all=true, not both');
  }

  const key       = makeUserProjectKey(userId, projectId);
  const container = getContainer();

  try {
    if (entryId) {
      // Single delete. We must know the doc id; reconstruct from convention.
      const docId = `${userId}_${projectId}_${entryId}`;
      try {
        await container.item(docId, key).delete();
        return ok({ deleted: 1, entryId });
      } catch (e) {
        if (e.code === 404) return err(404, `entry ${entryId} not found`);
        throw e;
      }
    }

    // Wipe-all. List all docs for the partition, delete each.
    const { resources } = await container.items
      .query({
        query: 'SELECT c.id FROM c WHERE c.userProjectKey = @key',
        parameters: [{ name: '@key', value: key }]
      }, { partitionKey: key })
      .fetchAll();

    let deleted = 0;
    const errors = [];
    for (const doc of (resources || [])) {
      try {
        await container.item(doc.id, key).delete();
        deleted++;
      } catch (e) {
        errors.push({ id: doc.id, error: e.message });
      }
    }

    return ok({
      deleted,
      failed: errors.length,
      errors: errors.length ? errors : undefined
    });
  } catch (e) {
    ctx.log(`[project-memory] DELETE failed: ${e.message}`);
    return err(500, e.message, (e.stack || '').split('\n').slice(0, 6).join('\n'));
  }
}

// ── Route registration ───────────────────────────────────────────────────────
app.http('project-memory', {
  methods:   ['GET', 'POST', 'DELETE', 'OPTIONS'],
  authLevel: 'function',
  route:     'agent/project-memory',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 204, headers: CORS, body: '' };

    try {
      if (req.method === 'GET')    return await handleGet(req, ctx);
      if (req.method === 'POST')   return await handlePost(req, ctx);
      if (req.method === 'DELETE') return await handleDelete(req, ctx);
      return err(405, `method ${req.method} not allowed`);
    } catch (e) {
      // Catch-all so an unhandled throw still returns a structured error.
      // In-band debugging: include stack since App Insights / Live Log /
      // Kudu are not available on this Flex Consumption plan.
      ctx.log(`[project-memory] uncaught: ${e.message}\n${e.stack || ''}`);
      return err(500, e.message, (e.stack || '').split('\n').slice(0, 6).join('\n'));
    }
  }
});
