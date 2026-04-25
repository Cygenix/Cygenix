// ─────────────────────────────────────────────────────────────────────────────
// agent.js — Agentive Migration backend (Stage 1: plumbing only)
// ─────────────────────────────────────────────────────────────────────────────
// Endpoints:
//   POST /api/agent/migrate              — Create a new agent run
//   GET  /api/agent/run/{runId}          — Read run state + messages
//   POST /api/agent/run/{runId}/respond  — User responds to an approval gate
//   POST /api/agent/run/{runId}/cancel   — Cancel a running agent
//
// Stage 1 limitations:
//   - The "agent loop" is stubbed. /migrate creates a run and immediately
//     marks it as awaiting_approval with a fake proposal so the frontend's
//     full lifecycle (running → approval → completed) can be tested.
//   - No Anthropic API calls yet. No SQL introspection yet.
//   - The shape of every Cosmos write matches what stage 2 will use, so
//     stage 2 only swaps the stub for the real loop without changing storage.
//
// Cosmos containers required (create in Azure Portal):
//   agent_runs      — partition key /userId
//   agent_messages  — partition key /runId
// ─────────────────────────────────────────────────────────────────────────────

const { app } = require('@azure/functions');
const crypto = require('crypto');

// ── Cosmos client (lazy singleton, matches index.js pattern) ────────────────
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

// ── Shared CORS headers (match index.js) ────────────────────────────────────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-user-id, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Content-Type': 'application/json'
};
const ok  = (body)      => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const err = (code, msg) => ({ status: code, headers: CORS, body: JSON.stringify({ error: msg }) });

function getUserId(req) {
  return req.headers.get('x-user-id') || req.query.get('userId') || null;
}

function nowIso() { return new Date().toISOString(); }
function shortId(prefix) {
  return `${prefix}_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
}

// ── Feature flag check ──────────────────────────────────────────────────────
// Reads the user's projects doc and looks for an aiAgentiveEnabled flag.
// Returns true by default in stage 1 so we don't need to set up the flag
// before testing. Tighten this in stage 2.
async function isAgentiveEnabledForUser(userId, ctx) {
  try {
    const { resource } = await getCosmosContainer('projects')
      .item(userId, userId).read();
    if (resource && resource.aiAgentiveEnabled === false) return false;
  } catch (e) {
    if (e.code !== 404) ctx.log('isAgentiveEnabledForUser read error:', e.message);
  }
  return true;
}

// ── Project connection lookup ────────────────────────────────────────────────
// Reads the user's stored connections from the `projects` container, same
// shape `data/save` and `data/load` use. Returns { srcConnString, tgtConnString,
// tgtFnUrl, tgtFnKey } or null if not configured.
async function getUserConnections(userId, ctx) {
  try {
    const { resource } = await getCosmosContainer('projects')
      .item(userId, userId).read();
    const conns = resource && resource.connections;
    if (!conns) return null;
    const srcConnString = conns.srcConnString || conns.source || '';
    const tgtConnString = conns.tgtConnString || conns.target || '';
    const tgtFnUrl      = conns.tgtFnUrl || '';
    const tgtFnKey      = conns.tgtFnKey || '';
    if (!srcConnString) return null;
    if (!tgtConnString && !tgtFnUrl) return null;
    return { srcConnString, tgtConnString, tgtFnUrl, tgtFnKey };
  } catch (e) {
    if (e.code === 404) return null;
    ctx.log('getUserConnections error:', e.message);
    return null;
  }
}

// ── Run document helpers ────────────────────────────────────────────────────
function newRunDoc({ userId, goal, conns }) {
  return {
    id: shortId('run'),
    userId,                          // partition key
    status: 'running',               // running | awaiting_approval | completed | failed | cancelled
    goal,
    direction: 'source_to_target',
    // Snapshot of resolved connections at run start. We store *fingerprints*,
    // not the credentials themselves, so the run history doesn't leak secrets.
    connectionsFingerprint: {
      sourceFingerprint: fingerprint(conns.srcConnString),
      targetFingerprint: fingerprint(conns.tgtConnString || conns.tgtFnUrl),
    },
    createdAt: nowIso(),
    updatedAt: nowIso(),
    pendingApproval: null,
    result: null,
    tokenUsage: { input: 0, output: 0, costUSD: 0 },
    budgetCap:  { maxTokens: 200_000, maxCostUSD: 2.00 },
  };
}

// Hash the first ~80 chars of a connection string so we can identify which
// connection a run used without storing the connection string itself.
function fingerprint(s) {
  if (!s) return null;
  return crypto.createHash('sha256').update(String(s).slice(0, 80)).digest('hex').slice(0, 12);
}

async function readRun(runId, userId) {
  try {
    const { resource } = await getCosmosContainer('agent_runs')
      .item(runId, userId).read();
    return resource || null;
  } catch (e) {
    if (e.code === 404) return null;
    throw e;
  }
}

async function writeRun(run) {
  run.updatedAt = nowIso();
  await getCosmosContainer('agent_runs').items.upsert(run);
  return run;
}

async function appendMessage(runId, message) {
  const doc = {
    id: shortId('msg'),
    runId,                           // partition key
    seq: message.seq != null ? message.seq : Date.now(),
    role: message.role,              // 'user' | 'assistant' | 'tool_result'
    content: message.content || null,
    toolName: message.toolName || null,
    toolInput: message.toolInput || null,
    toolResult: message.toolResult || null,
    createdAt: nowIso(),
  };
  await getCosmosContainer('agent_messages').items.create(doc);
  return doc;
}

async function loadMessages(runId, sinceSeq) {
  const container = getCosmosContainer('agent_messages');
  const query = sinceSeq && sinceSeq > 0
    ? {
        query: 'SELECT * FROM c WHERE c.runId = @runId AND c.seq > @seq ORDER BY c.seq ASC',
        parameters: [{ name: '@runId', value: runId }, { name: '@seq', value: Number(sinceSeq) }],
      }
    : {
        query: 'SELECT * FROM c WHERE c.runId = @runId ORDER BY c.seq ASC',
        parameters: [{ name: '@runId', value: runId }],
      };
  const { resources } = await container.items.query(query, { partitionKey: runId }).fetchAll();
  return resources;
}

// ─────────────────────────────────────────────────────────────────────────────
// ROUTE: POST /api/agent/migrate — Start a new agent run
// ─────────────────────────────────────────────────────────────────────────────
app.http('agent_migrate', {
  methods: ['POST', 'OPTIONS'],
  authLevel: 'function',
  route: 'agent/migrate',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 200, headers: CORS, body: '' };

    const userId = getUserId(req);
    if (!userId) return err(401, 'x-user-id header is required');

    if (!(await isAgentiveEnabledForUser(userId, ctx))) {
      return err(403, 'Agentive Migration is not enabled for this user');
    }

    const body = await req.json().catch(() => null);
    if (!body || typeof body !== 'object') return err(400, 'Invalid JSON body');
    const goal = (body.goal || '').trim();
    if (goal.length < 10) return err(400, 'goal must be at least 10 characters');

    const conns = await getUserConnections(userId, ctx);
    if (!conns) {
      return err(400, 'Source and target connections must be configured before starting an agent run.');
    }

    const run = newRunDoc({ userId, goal, conns });

    // Seed the conversation with an initial user message so the future agent
    // loop has context to work from.
    await getCosmosContainer('agent_runs').items.create(run);
    await appendMessage(run.id, {
      seq: 1,
      role: 'user',
      content: [{ type: 'text', text: buildInitialPrompt(goal, run) }],
    });

    ctx.log(`[agent] run ${run.id} created for user ${userId}, goal: "${goal.slice(0, 80)}…"`);

    // Stage 1 stub: synthesize a fake proposal so the frontend's approval flow
    // can be exercised end to end without an Anthropic API key being set up.
    // Stage 2 will replace this with a real agent loop.
    if (process.env.AGENT_STUB_MODE === '1') {
      await stubProduceFakeProposal(run, ctx);
    } else {
      // Without stub mode, we mark the run as failed with a clear message
      // because there's no real agent loop yet. Frontend will show this.
      run.status = 'failed';
      run.result = { error: 'Agent loop not deployed yet. Set AGENT_STUB_MODE=1 in Function app settings to test the UI flow with a fake proposal.' };
      await writeRun(run);
    }

    return ok({ runId: run.id });
  },
});

// Stage 1 stub: write a fake assistant turn that "proposes a mapping",
// then mark the run awaiting_approval so the frontend renders the approval view.
async function stubProduceFakeProposal(run, ctx) {
  await appendMessage(run.id, {
    seq: 2,
    role: 'assistant',
    content: [
      { type: 'text', text: 'Reading source schema to understand what we are migrating from.' },
      { type: 'tool_use', id: 'stub_t1', name: 'introspect_source_schema', input: {} },
    ],
  });
  await appendMessage(run.id, {
    seq: 3,
    role: 'user',
    content: [{ type: 'tool_result', tool_use_id: 'stub_t1', content: '(stub) Found 4 tables: customers, orders, order_items, products' }],
  });
  await appendMessage(run.id, {
    seq: 4,
    role: 'assistant',
    content: [
      { type: 'text', text: 'Now reading target schema to see what types are supported.' },
      { type: 'tool_use', id: 'stub_t2', name: 'introspect_target_schema', input: {} },
    ],
  });
  await appendMessage(run.id, {
    seq: 5,
    role: 'user',
    content: [{ type: 'tool_result', tool_use_id: 'stub_t2', content: '(stub) Target has matching schema with minor type differences' }],
  });

  run.status = 'awaiting_approval';
  run.pendingApproval = {
    type: 'propose_mapping',
    requestedAt: nowIso(),
    payload: {
      summary: 'Stub proposal — Stage 1 plumbing test. Maps 4 tables from source to target with no transformations. This is fake data to verify the approval UI works; the real agent loop is added in Stage 2.',
      decisions: [
        {
          decision: 'Map customers.email as VARCHAR(255)',
          reasoning: 'Source uses VARCHAR(MAX); target column is VARCHAR(255). No actual data exceeds 255 chars in current sample.',
          confidence: 'high',
        },
        {
          decision: 'Exclude customers.password_hash',
          reasoning: 'Target has its own auth system. Migrating hashed passwords would be both insecure and pointless.',
          confidence: 'high',
        },
        {
          decision: 'Coerce orders.created_at from BIGINT epoch to DATETIME2',
          reasoning: 'Source stores Unix timestamps as BIGINT. Target column is DATETIME2.',
          confidence: 'medium',
        },
      ],
      mapping: {
        tables: [
          { sourceTable: 'customers',  targetTable: 'customers',  columns: stubCols(8, 1) },
          { sourceTable: 'orders',     targetTable: 'orders',     columns: stubCols(6, 0) },
          { sourceTable: 'order_items',targetTable: 'order_items',columns: stubCols(5, 0) },
          { sourceTable: 'products',   targetTable: 'products',   columns: stubCols(7, 0) },
        ],
      },
    },
  };
  await writeRun(run);
  ctx.log(`[agent] run ${run.id} produced stub proposal, awaiting approval`);
}

function stubCols(n, excluded) {
  const cols = [];
  for (let i = 0; i < n; i++) {
    cols.push({ sourceColumn: `col_${i}`, targetColumn: `col_${i}`, sourceType: 'VARCHAR', targetType: 'VARCHAR', excluded: i < excluded });
  }
  return cols;
}

function buildInitialPrompt(goal, run) {
  return `Migrate from source to target.

User goal: ${goal}

Run id: ${run.id}
Source fingerprint: ${run.connectionsFingerprint.sourceFingerprint}
Target fingerprint: ${run.connectionsFingerprint.targetFingerprint}

Use the introspect tools to understand the schemas, then propose a mapping with reasoning for each non-trivial decision. Pause for the user via ask_user when you cannot make a confident decision on your own.`;
}

// ─────────────────────────────────────────────────────────────────────────────
// ROUTE: GET /api/agent/run/{runId} — Read run state + messages
// ─────────────────────────────────────────────────────────────────────────────
app.http('agent_run_read', {
  methods: ['GET', 'OPTIONS'],
  authLevel: 'function',
  route: 'agent/run/{runId}',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 200, headers: CORS, body: '' };

    const userId = getUserId(req);
    if (!userId) return err(401, 'x-user-id header is required');

    const runId = req.params.runId;
    if (!runId) return err(400, 'runId required');

    const run = await readRun(runId, userId);
    if (!run) return err(404, 'Run not found');

    const sinceSeq = parseInt(req.query.get('sinceSeq') || '0', 10) || 0;
    const messages = await loadMessages(runId, sinceSeq);
    const allMessages = sinceSeq > 0 ? await loadMessages(runId, 0) : messages;

    return ok({ run, messages, allMessages });
  },
});

// ─────────────────────────────────────────────────────────────────────────────
// ROUTE: POST /api/agent/run/{runId}/respond — Approve / edit / reject / answer
// ─────────────────────────────────────────────────────────────────────────────
app.http('agent_run_respond', {
  methods: ['POST', 'OPTIONS'],
  authLevel: 'function',
  route: 'agent/run/{runId}/respond',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 200, headers: CORS, body: '' };

    const userId = getUserId(req);
    if (!userId) return err(401, 'x-user-id header is required');

    const runId = req.params.runId;
    const run = await readRun(runId, userId);
    if (!run) return err(404, 'Run not found');
    if (run.status !== 'awaiting_approval') {
      return err(409, `Run is not awaiting approval (status: ${run.status})`);
    }

    const body = await req.json().catch(() => null);
    if (!body || typeof body !== 'object') return err(400, 'Invalid JSON body');
    const action = body.action;

    switch (action) {
      case 'approve': {
        // Stage 1: persist the proposed mapping into the user's projects doc
        // so it appears in Object Mapping. Stage 2 will replace this with a
        // proper write to a dedicated mappings container.
        const mapping = run.pendingApproval && run.pendingApproval.payload && run.pendingApproval.payload.mapping;
        const mappingId = await saveProposedMapping(userId, mapping, run, ctx);
        run.status = 'completed';
        run.pendingApproval = null;
        run.result = {
          mappingId,
          summary: `Saved a mapping with ${mapping && mapping.tables ? mapping.tables.length : 0} tables.`,
        };
        await writeRun(run);
        return ok({ ok: true, status: 'completed', mappingId });
      }
      case 'edit': {
        const mapping = body.mapping || (run.pendingApproval && run.pendingApproval.payload && run.pendingApproval.payload.mapping);
        const mappingId = await saveProposedMapping(userId, mapping, run, ctx);
        run.status = 'completed';
        run.pendingApproval = null;
        run.result = { mappingId, summary: 'Mapping saved with your edits.' };
        await writeRun(run);
        return ok({ ok: true, status: 'completed', mappingId });
      }
      case 'reject': {
        // Stage 1 stub: reject just marks the run failed. Stage 2 will feed
        // the feedback back into the agent loop as a tool_result so the
        // agent can try again.
        run.status = 'failed';
        run.pendingApproval = null;
        run.result = { error: `Rejected: ${body.feedback || 'no feedback given'}. (Stage 2 will let the agent retry with this feedback.)` };
        await writeRun(run);
        return ok({ ok: true, status: 'failed' });
      }
      case 'answer': {
        // Stage 1 stub: ask_user answers don't lead anywhere yet because
        // there's no real agent loop. Mark cancelled.
        run.status = 'cancelled';
        run.pendingApproval = null;
        await writeRun(run);
        return ok({ ok: true, status: 'cancelled' });
      }
      default:
        return err(400, `Unknown action: ${action}`);
    }
  },
});

// Stage 1: write the approved mapping into the user's projects doc under a
// new `agent_mappings` field. Returns a synthetic mappingId.
async function saveProposedMapping(userId, mapping, run, ctx) {
  const mappingId = shortId('map');
  if (!mapping) return mappingId;

  const container = getCosmosContainer('projects');
  let existing = {};
  try {
    const { resource } = await container.item(userId, userId).read();
    existing = resource || {};
  } catch (e) {
    if (e.code !== 404) throw e;
  }

  const list = Array.isArray(existing.agent_mappings) ? existing.agent_mappings : [];
  list.push({
    id: mappingId,
    runId: run.id,
    createdAt: nowIso(),
    summary: (run.pendingApproval && run.pendingApproval.payload && run.pendingApproval.payload.summary) || '',
    mapping,
  });
  existing.agent_mappings = list;
  existing.id = userId;
  existing.userId = userId;
  existing.updatedAt = nowIso();
  await container.items.upsert(existing);

  ctx.log(`[agent] saved mapping ${mappingId} for user ${userId} (run ${run.id})`);
  return mappingId;
}

// ─────────────────────────────────────────────────────────────────────────────
// ROUTE: POST /api/agent/run/{runId}/cancel
// ─────────────────────────────────────────────────────────────────────────────
app.http('agent_run_cancel', {
  methods: ['POST', 'OPTIONS'],
  authLevel: 'function',
  route: 'agent/run/{runId}/cancel',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 200, headers: CORS, body: '' };

    const userId = getUserId(req);
    if (!userId) return err(401, 'x-user-id header is required');

    const runId = req.params.runId;
    const run = await readRun(runId, userId);
    if (!run) return err(404, 'Run not found');

    if (run.status === 'completed' || run.status === 'failed' || run.status === 'cancelled') {
      return ok({ ok: true, status: run.status });   // already terminal
    }

    run.status = 'cancelled';
    run.pendingApproval = null;
    await writeRun(run);
    ctx.log(`[agent] run ${runId} cancelled by user ${userId}`);
    return ok({ ok: true, status: 'cancelled' });
  },
});

// Note: this module has no exports. It registers HTTP routes via app.http()
// at load time, so simply requiring the file from index.js is enough.
