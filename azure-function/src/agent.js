// ─────────────────────────────────────────────────────────────────────────────
// agent.js — Agentive Migration backend
// ─────────────────────────────────────────────────────────────────────────────
// Endpoints:
//   POST /api/agent/migrate              — Create a new agent run
//   GET  /api/agent/run/{runId}          — Read run state + messages
//   POST /api/agent/run/{runId}/respond  — User responds to an approval gate
//   POST /api/agent/run/{runId}/cancel   — Cancel a running agent
//
// Modes (controlled by AGENT_STUB_MODE env var):
//   AGENT_STUB_MODE=1  — Stage 1 stub: synthesize a fake proposal so the UI
//                        flow can be exercised without an Anthropic call.
//   AGENT_STUB_MODE=0  — Stage 2a: call Anthropic for a real response, no
//                        tools yet. Returns the model's text reply as the
//                        proposal summary so the existing approval UI works.
//   (unset, default 0) — Same as 0.
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

// ── Anthropic client (lazy singleton) ────────────────────────────────────────
let _anthropic = null;
function getAnthropic() {
  if (!_anthropic) {
    const Anthropic = require('@anthropic-ai/sdk');
    if (!process.env.ANTHROPIC_API_KEY) {
      throw new Error('ANTHROPIC_API_KEY is not configured in Function app settings');
    }
    _anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
  }
  return _anthropic;
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
function isStubMode() { return process.env.AGENT_STUB_MODE === '1'; }

// ── Feature flag check ──────────────────────────────────────────────────────
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
    userId,
    status: 'running',
    goal,
    direction: 'source_to_target',
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
    mode: isStubMode() ? 'stub' : 'live',
  };
}

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
    runId,
    seq: message.seq != null ? message.seq : Date.now(),
    role: message.role,
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

// ── Token cost estimation (claude-sonnet-4-5 pricing) ───────────────────────
// As of writing: $3/MTok input, $15/MTok output. Update if pricing changes.
function estimateCostUSD(usage) {
  const inputCost  = (usage.input_tokens  || 0) * (3.00 / 1_000_000);
  const outputCost = (usage.output_tokens || 0) * (15.00 / 1_000_000);
  return inputCost + outputCost;
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
    await getCosmosContainer('agent_runs').items.create(run);
    await appendMessage(run.id, {
      seq: 1,
      role: 'user',
      content: [{ type: 'text', text: buildInitialPrompt(goal, run) }],
    });

    ctx.log(`[agent] run ${run.id} created (mode=${run.mode}) for user ${userId}, goal: "${goal.slice(0, 80)}"`);

    // Branch on mode. Both branches end with the run in awaiting_approval state
    // so the frontend's approval UI handles both transparently.
    try {
      if (isStubMode()) {
        await stubProduceFakeProposal(run, ctx);
      } else {
        await liveProduceFirstProposal(run, ctx);
      }
    } catch (e) {
      // Catch-all: if the live agent throws (Anthropic outage, bad key, etc),
      // mark the run failed with a clean error message rather than 500ing.
      ctx.log(`[agent] run ${run.id} failed: ${e.message}`);
      run.status = 'failed';
      run.result = { error: e.message };
      await writeRun(run);
    }

    return ok({ runId: run.id });
  },
});

// ─────────────────────────────────────────────────────────────────────────────
// STAGE 1: Stub mode — synthesize a fake proposal without calling Anthropic.
// Used when AGENT_STUB_MODE=1. Lets us test the full UI flow without API cost.
// ─────────────────────────────────────────────────────────────────────────────
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

// ─────────────────────────────────────────────────────────────────────────────
// STAGE 2a: Live mode — call Anthropic for a real response, no tools yet.
//
// What this DOES:
//   - Calls Claude with the user's goal + a system prompt
//   - Stores the model's text response as an assistant message
//   - Logs token usage to the run doc
//   - Sets status to awaiting_approval with the model's text in the summary
//
// What this DOES NOT do (yet):
//   - Tool use (introspect_source_schema, propose_mapping, etc.) — Stage 2b
//   - Multi-turn agent loop — Stage 2b
//   - Real schema introspection — Stage 2b
// ─────────────────────────────────────────────────────────────────────────────
async function liveProduceFirstProposal(run, ctx) {
  const anthropic = getAnthropic();
  const model = process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-5';

  const systemPrompt = STAGE_2A_SYSTEM_PROMPT;
  const initialMsg = `User goal:\n\n${run.goal}\n\n` +
    `Source database fingerprint: ${run.connectionsFingerprint.sourceFingerprint}\n` +
    `Target database fingerprint: ${run.connectionsFingerprint.targetFingerprint}\n\n` +
    `In Stage 2a you do not yet have schema introspection tools available. ` +
    `Acknowledge the goal, describe the high-level approach you would take, and list ` +
    `the kinds of decisions you expect to make once you can see the schemas. Keep your ` +
    `response under 300 words.`;

  ctx.log(`[agent] run ${run.id} calling Anthropic (model=${model})...`);

  const t0 = Date.now();
  let response;
  try {
    response = await anthropic.messages.create({
      model,
      max_tokens: 1024,
      system: systemPrompt,
      messages: [{ role: 'user', content: initialMsg }],
    });
  } catch (e) {
    // Anthropic errors carry useful structured info — surface the relevant bits
    const detail = e.status ? `${e.status} ${e.message}` : e.message;
    throw new Error(`Anthropic API call failed: ${detail}`);
  }
  const elapsed = Date.now() - t0;

  // Extract text blocks from the response (response.content is an array of
  // typed blocks; for a no-tool call there should just be one or more text blocks).
  const textBlocks = (response.content || []).filter(b => b.type === 'text');
  const fullText = textBlocks.map(b => b.text).join('\n').trim();
  if (!fullText) {
    throw new Error('Anthropic returned no text content');
  }

  // Update token usage and cost on the run doc
  const usage = response.usage || {};
  const cost  = estimateCostUSD(usage);
  run.tokenUsage = {
    input:   (run.tokenUsage.input  || 0) + (usage.input_tokens  || 0),
    output:  (run.tokenUsage.output || 0) + (usage.output_tokens || 0),
    costUSD: (run.tokenUsage.costUSD || 0) + cost,
  };

  ctx.log(`[agent] run ${run.id} Anthropic response in ${elapsed}ms, ` +
    `${usage.input_tokens || 0} in + ${usage.output_tokens || 0} out tokens, ` +
    `$${cost.toFixed(4)}`);

  // Persist the assistant turn so the frontend activity log shows it
  await appendMessage(run.id, {
    seq: 2,
    role: 'assistant',
    content: response.content,
  });

  // Stage 2a returns the model's text as the proposal summary so the existing
  // approval UI renders it. The "decisions" and "mapping" arrays are placeholders
  // since we don't have real introspection yet — they get a single placeholder
  // entry to make the UI show context, not blank space.
  run.status = 'awaiting_approval';
  run.pendingApproval = {
    type: 'propose_mapping',
    requestedAt: nowIso(),
    payload: {
      summary: fullText,
      decisions: [
        {
          decision: 'Stage 2a — model is reachable and responding',
          reasoning: 'This is the first end-to-end live call. Schema introspection and structured proposals come in Stage 2b/2c.',
          confidence: 'high',
        },
      ],
      mapping: { tables: [] },  // empty until Stage 2b adds introspection
    },
  };
  await writeRun(run);
  ctx.log(`[agent] run ${run.id} live proposal produced, awaiting approval`);
}

const STAGE_2A_SYSTEM_PROMPT = `You are the Cygenix Migration Agent, an AI assistant that helps users migrate SQL databases.

Cygenix is a SaaS platform where users connect a source SQL Server database and a target SQL Server database, then ask the agent to propose a mapping for migrating data from source to target. The agent introspects both schemas, proposes column-level mappings with reasoning, asks the user for clarification on ambiguous decisions, and only acts after explicit approval.

Principles you follow:
- Prefer exact type matches. Coerce only when necessary.
- Flag potential PII columns (ssn, email, dob, credit card, etc.) for user review.
- Preserve foreign key relationships. Order operations to respect dependencies.
- Be conservative: when uncertain, ask. The user prefers a slow correct migration to a fast wrong one.
- Never fabricate column or table names — only reference what introspection actually returned.

You are currently running in Stage 2a — you have no tools yet. Acknowledge the user's goal, describe the approach you would take, and list the kinds of decisions you expect to face once schema introspection is available. Be concise.`;

function buildInitialPrompt(goal, run) {
  return `Migrate from source to target.

User goal: ${goal}

Run id: ${run.id}
Source fingerprint: ${run.connectionsFingerprint.sourceFingerprint}
Target fingerprint: ${run.connectionsFingerprint.targetFingerprint}`;
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
// ROUTE: POST /api/agent/run/{runId}/respond
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
        run.status = 'failed';
        run.pendingApproval = null;
        run.result = { error: `Rejected: ${body.feedback || 'no feedback given'}.` };
        await writeRun(run);
        return ok({ ok: true, status: 'failed' });
      }
      case 'answer': {
        // Stage 2a: ask_user not used yet; mark cancelled if we ever hit this
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
      return ok({ ok: true, status: run.status });
    }

    run.status = 'cancelled';
    run.pendingApproval = null;
    await writeRun(run);
    ctx.log(`[agent] run ${runId} cancelled by user ${userId}`);
    return ok({ ok: true, status: 'cancelled' });
  },
});
