// agent-scope.js
//
// POST   /api/agent/scope                       — create / update a scope
// GET    /api/agent/scope?projectId=X           — list scopes for a project
// GET    /api/agent/scope/{scopeId}             — fetch one scope
// DELETE /api/agent/scope/{scopeId}             — delete a scope
// POST   /api/agent/scope/expand                — compute FK closure for a scope
// GET    /api/agent/scope/{scopeId}/effective   — return root + expanded as flat list with levels
//
// Scope filters the planner's full target schema down to the engagement's
// real migration target set. See docs/agent-scope-spec.md for the full design.
//
// Cosmos containers used:
//   agent_scopes        (new)        partitioned on /userId
//   agent_plans         (existing)   read-only — to expand FK closure
//   agent_plan_levels   (existing)   read-only — to walk per-level edge data
//
// Module templates are loaded from module-templates.json colocated with this
// file. v1 ships the file with all module entries empty; callers can still
// reference module names but they contribute zero tables until populated.

const { app } = require('@azure/functions');
const path    = require('path');
const fs      = require('fs');

// ── CORS ─────────────────────────────────────────────────────────────────────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-user-id',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Content-Type':                 'application/json'
};

const ok  = (body)             => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const err = (code, msg, stack) => ({
  status: code,
  headers: CORS,
  body: JSON.stringify({ error: msg, ...(stack ? { stack } : {}) })
});

// ── Cosmos client ────────────────────────────────────────────────────────────
let _cosmos = null;
function getContainer(name) {
  if (!_cosmos) {
    const { CosmosClient } = require('@azure/cosmos');
    _cosmos = new CosmosClient({
      endpoint: process.env.COSMOS_ENDPOINT,
      key:      process.env.COSMOS_KEY
    });
  }
  return _cosmos
    .database(process.env.COSMOS_DATABASE || 'cygenix')
    .container(name);
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function getUserId(req) {
  return req.headers.get('x-user-id') || req.query.get('userId') || null;
}

function nowIso() { return new Date().toISOString(); }

function shortId(prefix) {
  const crypto = require('crypto');
  return `${prefix}_${crypto.randomBytes(6).toString('hex')}`;
}

// ── Module templates ─────────────────────────────────────────────────────────
// Lazy-loaded once per process. Hot-reloading would require fs.watch which is
// overkill for a JSON file that rarely changes; restart the function to pick
// up edits.
let _moduleTemplates = null;
function getModuleTemplates() {
  if (_moduleTemplates !== null) return _moduleTemplates;
  try {
    const filePath = path.join(__dirname, 'module-templates.json');
    const raw = fs.readFileSync(filePath, 'utf8');
    _moduleTemplates = JSON.parse(raw);
  } catch (e) {
    // Missing or malformed file is non-fatal — modules just contribute zero
    // tables. Log so it's diagnosable.
    console.warn(`[scope] module-templates.json unavailable: ${e.message}`);
    _moduleTemplates = {};
  }
  return _moduleTemplates;
}

// ── Validation ───────────────────────────────────────────────────────────────
function validateScopeInput(input) {
  if (!input || typeof input !== 'object') {
    return { ok: false, code: 400, msg: 'scope body is required' };
  }
  if (!input.projectId || typeof input.projectId !== 'string') {
    return { ok: false, code: 400, msg: 'projectId is required' };
  }
  if (!input.name || typeof input.name !== 'string' || input.name.length > 200) {
    return { ok: false, code: 400, msg: 'name is required (non-empty, max 200 chars)' };
  }
  if (!Array.isArray(input.rootTables)) {
    return { ok: false, code: 400, msg: 'rootTables must be an array (may be empty)' };
  }
  if (input.rootTables.length > 5000) {
    return { ok: false, code: 400, msg: 'rootTables exceeds 5000 entries — split into multiple scopes' };
  }
  for (const t of input.rootTables) {
    if (typeof t !== 'string' || !t.includes('.')) {
      return { ok: false, code: 400, msg: `rootTables entries must be "schema.table" strings; got: ${JSON.stringify(t)}` };
    }
  }
  return { ok: true };
}

// ── FK closure walk ──────────────────────────────────────────────────────────
//
// Given a plan and a set of root tables, compute the set of all tables those
// roots transitively depend on via declared FKs. Reads from agent_plan_levels
// in Cosmos — does NOT re-introspect the database.
//
// Returns { expandedTables: [...], stats: { tablesRead, edgesWalked, ... } }.
async function computeFkClosure(planId, rootTables, options, ctx) {
  const includeChildren = !!(options && options.includeChildren);

  // Fetch all level docs for this plan in one cross-document partition query.
  // agent_plan_levels is partitioned by planId so this is a single-partition
  // read and cheap.
  const { resources: levelDocs } = await getContainer('agent_plan_levels').items
    .query({
      query: 'SELECT * FROM c WHERE c.planId = @pid',
      parameters: [{ name: '@pid', value: planId }]
    }, { partitionKey: planId })
    .fetchAll();

  if (!levelDocs || levelDocs.length === 0) {
    return { expandedTables: [], stats: { tablesRead: 0, edgesWalked: 0, error: 'plan has no level documents' } };
  }

  // Build adjacency lists:
  //   outgoingByTable: A -> Set of tables A depends on (FK targets)
  //   incomingByTable: A -> Set of tables that depend on A (FK sources)
  const outgoingByTable = new Map();
  const incomingByTable = new Map();
  let tablesRead = 0;

  for (const lvl of levelDocs) {
    for (const t of (lvl.tables || [])) {
      tablesRead++;
      if (!outgoingByTable.has(t.fullName)) outgoingByTable.set(t.fullName, new Set());
      for (const e of (t.outgoingEdges || [])) {
        // Skip soft-reference edges by default — they're not load-order
        // constraints. (createdBy/modifiedBy etc.)
        if (e.softReference) continue;
        // Skip very-low-confidence inferred edges (under 0.5 wouldn't appear
        // in the graph anyway, but belt-and-braces).
        if (!e.declared && (e.confidence || 0) < 0.5) continue;

        outgoingByTable.get(t.fullName).add(e.to);
        if (!incomingByTable.has(e.to)) incomingByTable.set(e.to, new Set());
        incomingByTable.get(e.to).add(t.fullName);
      }
    }
  }

  // BFS from each root, following outgoing edges (ancestors). Optionally
  // also follow incoming edges (descendants) if includeChildren is set.
  const expanded = new Set();   // does NOT include the roots themselves
  const visited = new Set(rootTables);   // roots are visited; we record expansion separately
  const queue = [...rootTables];
  let edgesWalked = 0;

  while (queue.length > 0) {
    const cur = queue.shift();
    // Outgoing (mandatory — load-order parents)
    for (const target of (outgoingByTable.get(cur) || [])) {
      edgesWalked++;
      if (!visited.has(target)) {
        visited.add(target);
        expanded.add(target);
        queue.push(target);
      }
    }
    // Incoming (optional — load-order children)
    if (includeChildren) {
      for (const source of (incomingByTable.get(cur) || [])) {
        edgesWalked++;
        if (!visited.has(source)) {
          visited.add(source);
          expanded.add(source);
          queue.push(source);
        }
      }
    }
  }

  ctx.log(`[scope] expansion: ${rootTables.length} roots → ${expanded.size} expanded (children=${includeChildren}, tablesRead=${tablesRead}, edgesWalked=${edgesWalked})`);

  return {
    expandedTables: [...expanded].sort(),
    stats: { tablesRead, edgesWalked, rootCount: rootTables.length, expandedCount: expanded.size }
  };
}

// ── HTTP handlers ────────────────────────────────────────────────────────────

// POST /api/agent/scope — create or update a scope
async function handleCreateOrUpdate(req, ctx) {
  const userId = getUserId(req);
  if (!userId) return err(401, 'x-user-id header is required');

  let body;
  try { body = await req.json(); }
  catch { return err(400, 'request body must be valid JSON'); }

  const v = validateScopeInput(body);
  if (!v.ok) return err(v.code, v.msg);

  const container = getContainer('agent_scopes');

  // Idempotent on (userId, projectId, name) — look up existing first.
  let existing = null;
  try {
    const { resources } = await container.items
      .query({
        query: `SELECT TOP 1 * FROM c
                WHERE c.userId = @uid
                  AND c.projectId = @pid
                  AND c.name = @name`,
        parameters: [
          { name: '@uid',  value: userId },
          { name: '@pid',  value: body.projectId },
          { name: '@name', value: body.name }
        ]
      }, { partitionKey: userId })
      .fetchAll();
    existing = (resources && resources[0]) || null;
  } catch (e) {
    ctx.log(`[scope] existing-scope lookup failed (proceeding to create): ${e.message}`);
  }

  const now = nowIso();
  let doc;
  if (existing) {
    doc = {
      ...existing,
      name:        body.name,
      description: body.description || existing.description || '',
      rootTables:  body.rootTables,
      // Expansion is invalidated when roots change. Caller must re-expand.
      expandedTables: [],
      expansion:      null,
      updatedAt:   now
    };
  } else {
    const scopeId = shortId('scope');
    doc = {
      id:          `${userId}_${scopeId}`,
      userId,
      projectId:   body.projectId,
      scopeId,
      name:        body.name,
      description: body.description || '',
      rootTables:  body.rootTables,
      expandedTables: [],
      expansion:   null,
      createdAt:   now,
      updatedAt:   now
    };
  }

  try {
    await container.items.upsert(doc);
    return ok({ saved: true, scope: doc, created: !existing });
  } catch (e) {
    ctx.log(`[scope] upsert failed: ${e.message}`);
    return err(500, e.message, (e.stack || '').split('\n').slice(0, 6).join('\n'));
  }
}

// GET /api/agent/scope?projectId=X — list scopes
async function handleList(req, ctx) {
  const userId = getUserId(req);
  if (!userId) return err(401, 'x-user-id header is required');

  const projectId = req.query.get('projectId');

  let querySpec;
  if (projectId) {
    querySpec = {
      query: `SELECT c.scopeId, c.projectId, c.name, c.description, c.createdAt, c.updatedAt,
                     ARRAY_LENGTH(c.rootTables) AS rootCount,
                     ARRAY_LENGTH(c.expandedTables) AS expandedCount,
                     c.expansion
              FROM c
              WHERE c.userId = @uid AND c.projectId = @pid
              ORDER BY c.updatedAt DESC`,
      parameters: [
        { name: '@uid', value: userId },
        { name: '@pid', value: projectId }
      ]
    };
  } else {
    querySpec = {
      query: `SELECT c.scopeId, c.projectId, c.name, c.description, c.createdAt, c.updatedAt,
                     ARRAY_LENGTH(c.rootTables) AS rootCount,
                     ARRAY_LENGTH(c.expandedTables) AS expandedCount,
                     c.expansion
              FROM c
              WHERE c.userId = @uid
              ORDER BY c.updatedAt DESC`,
      parameters: [{ name: '@uid', value: userId }]
    };
  }

  try {
    const { resources } = await getContainer('agent_scopes').items
      .query(querySpec, { partitionKey: userId })
      .fetchAll();
    return ok({ scopes: resources || [] });
  } catch (e) {
    ctx.log(`[scope] list failed: ${e.message}`);
    return err(500, e.message);
  }
}

// GET /api/agent/scope/{scopeId}
async function handleRead(req, ctx, scopeId) {
  const userId = getUserId(req);
  if (!userId) return err(401, 'x-user-id header is required');

  try {
    const { resource } = await getContainer('agent_scopes')
      .item(`${userId}_${scopeId}`, userId).read();
    if (!resource) return err(404, `scope ${scopeId} not found`);
    return ok(resource);
  } catch (e) {
    if (e.code === 404) return err(404, `scope ${scopeId} not found`);
    ctx.log(`[scope] read failed: ${e.message}`);
    return err(500, e.message);
  }
}

// DELETE /api/agent/scope/{scopeId}
async function handleDelete(req, ctx, scopeId) {
  const userId = getUserId(req);
  if (!userId) return err(401, 'x-user-id header is required');

  try {
    await getContainer('agent_scopes').item(`${userId}_${scopeId}`, userId).delete();
    return ok({ deleted: true, scopeId });
  } catch (e) {
    if (e.code === 404) return err(404, `scope ${scopeId} not found`);
    ctx.log(`[scope] delete failed: ${e.message}`);
    return err(500, e.message);
  }
}

// POST /api/agent/scope/expand — compute FK closure and persist into scope
async function handleExpand(req, ctx) {
  const userId = getUserId(req);
  if (!userId) return err(401, 'x-user-id header is required');

  let body;
  try { body = await req.json(); }
  catch { return err(400, 'request body must be valid JSON'); }

  if (!body || !body.scopeId) return err(400, 'scopeId is required');
  if (!body.planId)           return err(400, 'planId is required');

  // Verify scope exists and belongs to this user.
  let scope;
  try {
    const { resource } = await getContainer('agent_scopes')
      .item(`${userId}_${body.scopeId}`, userId).read();
    scope = resource;
  } catch (e) {
    if (e.code === 404) return err(404, `scope ${body.scopeId} not found`);
    throw e;
  }
  if (!scope) return err(404, `scope ${body.scopeId} not found`);

  // Verify plan exists and belongs to this user (security boundary —
  // agent_plan_levels is partitioned by planId, not userId, so a user could
  // otherwise expand against another user's plan if they guessed the id).
  try {
    const { resource: plan } = await getContainer('agent_plans')
      .item(`${userId}_${body.planId}`, userId).read();
    if (!plan) return err(404, `plan ${body.planId} not found`);
  } catch (e) {
    if (e.code === 404) return err(404, `plan ${body.planId} not found`);
    throw e;
  }

  // Merge in any module-template root tables.
  const moduleNames = Array.isArray(body.moduleTemplates) ? body.moduleTemplates : [];
  const moduleTemplates = getModuleTemplates();
  const moduleRoots = new Set();
  for (const m of moduleNames) {
    const tables = moduleTemplates[m];
    if (Array.isArray(tables)) {
      for (const t of tables) moduleRoots.add(t);
    } else {
      ctx.log(`[scope] unknown module template '${m}' — ignoring`);
    }
  }

  const rootSet = new Set([...(scope.rootTables || []), ...moduleRoots]);
  const allRoots = [...rootSet];

  // Run the FK closure
  let expansion;
  try {
    expansion = await computeFkClosure(body.planId, allRoots, {
      includeChildren: !!body.includeChildren
    }, ctx);
  } catch (e) {
    ctx.log(`[scope] closure failed: ${e.message}`);
    return err(500, `FK closure expansion failed: ${e.message}`,
      (e.stack || '').split('\n').slice(0, 6).join('\n'));
  }

  // Persist the expansion back onto the scope
  const updated = {
    ...scope,
    rootTables:     allRoots,
    expandedTables: expansion.expandedTables,
    expansion: {
      planId:              body.planId,
      expandedAt:          nowIso(),
      includeChildren:     !!body.includeChildren,
      moduleTemplatesUsed: moduleNames,
      stats:               expansion.stats
    },
    updatedAt: nowIso()
  };

  try {
    await getContainer('agent_scopes').items.upsert(updated);
  } catch (e) {
    ctx.log(`[scope] upsert after expansion failed: ${e.message}`);
    return err(500, e.message);
  }

  return ok({ scope: updated });
}

// GET /api/agent/scope/{scopeId}/effective — root + expanded as flat list with levels
async function handleEffective(req, ctx, scopeId) {
  const userId = getUserId(req);
  if (!userId) return err(401, 'x-user-id header is required');

  // Fetch scope
  let scope;
  try {
    const { resource } = await getContainer('agent_scopes')
      .item(`${userId}_${scopeId}`, userId).read();
    scope = resource;
  } catch (e) {
    if (e.code === 404) return err(404, `scope ${scopeId} not found`);
    throw e;
  }
  if (!scope) return err(404, `scope ${scopeId} not found`);

  if (!scope.expansion || !scope.expansion.planId) {
    return err(400, 'scope has not been expanded yet. POST /api/agent/scope/expand first.');
  }

  // Pull the plan's level docs to determine each table's level.
  let levelDocs;
  try {
    const { resources } = await getContainer('agent_plan_levels').items
      .query({
        query: 'SELECT * FROM c WHERE c.planId = @pid',
        parameters: [{ name: '@pid', value: scope.expansion.planId }]
      }, { partitionKey: scope.expansion.planId })
      .fetchAll();
    levelDocs = resources || [];
  } catch (e) {
    ctx.log(`[scope] effective: level fetch failed: ${e.message}`);
    return err(500, e.message);
  }

  const inScope = new Set([...(scope.rootTables || []), ...(scope.expandedTables || [])]);
  const effective = [];

  for (const lvl of levelDocs) {
    for (const t of (lvl.tables || [])) {
      if (!inScope.has(t.fullName)) continue;
      effective.push({
        fullName: t.fullName,
        schema:   t.schema,
        name:     t.name,
        rows:     t.rows,
        level:    lvl.level,
        theme:    t.theme,
        isNoise:  t.isNoise,
        isRoot:   (scope.rootTables || []).includes(t.fullName)
      });
    }
  }

  // Sort by level ascending, then by name. Loaded in this order; UI can re-sort.
  effective.sort((a, b) => a.level - b.level || a.fullName.localeCompare(b.fullName));

  return ok({
    scopeId,
    planId:    scope.expansion.planId,
    rootCount: (scope.rootTables || []).length,
    expandedCount: (scope.expandedTables || []).length,
    totalCount: effective.length,
    tables:    effective
  });
}

// ── Route registration ───────────────────────────────────────────────────────

// /agent/scope and /agent/scope/expand
app.http('agent-scope', {
  methods:   ['GET', 'POST', 'OPTIONS'],
  authLevel: 'function',
  route:     'agent/scope',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 204, headers: CORS, body: '' };
    try {
      if (req.method === 'POST') return await handleCreateOrUpdate(req, ctx);
      if (req.method === 'GET')  return await handleList(req, ctx);
      return err(405, `method ${req.method} not allowed on /agent/scope`);
    } catch (e) {
      ctx.log(`[scope] uncaught: ${e.message}\n${e.stack || ''}`);
      return err(500, e.message, (e.stack || '').split('\n').slice(0, 6).join('\n'));
    }
  }
});

// Dedicated route for the expand action so it can't collide with /agent/scope/{scopeId}
app.http('agent-scope-expand', {
  methods:   ['POST', 'OPTIONS'],
  authLevel: 'function',
  route:     'agent/scope/expand',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 204, headers: CORS, body: '' };
    try {
      return await handleExpand(req, ctx);
    } catch (e) {
      ctx.log(`[scope-expand] uncaught: ${e.message}\n${e.stack || ''}`);
      return err(500, e.message, (e.stack || '').split('\n').slice(0, 6).join('\n'));
    }
  }
});

// Per-scope read / delete
app.http('agent-scope-item', {
  methods:   ['GET', 'DELETE', 'OPTIONS'],
  authLevel: 'function',
  route:     'agent/scope/{scopeId}',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 204, headers: CORS, body: '' };
    const scopeId = req.params.scopeId;
    // Guard against the 'expand' segment being routed here by URL-matching
    // precedence. expand has its own route above so this should not fire,
    // but defence in depth costs nothing.
    if (scopeId === 'expand') return err(400, 'use POST /agent/scope/expand for the expand action');
    try {
      if (req.method === 'GET')    return await handleRead(req, ctx, scopeId);
      if (req.method === 'DELETE') return await handleDelete(req, ctx, scopeId);
      return err(405, `method ${req.method} not allowed`);
    } catch (e) {
      ctx.log(`[scope-item] uncaught: ${e.message}\n${e.stack || ''}`);
      return err(500, e.message, (e.stack || '').split('\n').slice(0, 6).join('\n'));
    }
  }
});

// Effective scope (flat list with levels)
app.http('agent-scope-effective', {
  methods:   ['GET', 'OPTIONS'],
  authLevel: 'function',
  route:     'agent/scope/{scopeId}/effective',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 204, headers: CORS, body: '' };
    const scopeId = req.params.scopeId;
    try {
      return await handleEffective(req, ctx, scopeId);
    } catch (e) {
      ctx.log(`[scope-effective] uncaught: ${e.message}\n${e.stack || ''}`);
      return err(500, e.message, (e.stack || '').split('\n').slice(0, 6).join('\n'));
    }
  }
});

// Export the closure helper for direct use by other modules (eventually the
// agent itself, when running mappings against an effective scope).
module.exports = { computeFkClosure };
