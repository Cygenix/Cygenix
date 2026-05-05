// agent-suggest-criteria.js
//
// POST /api/agent/suggest-criteria?code=<FUNC_KEY>
//
// Called eagerly when the user picks a group card on the Agentive Migration
// page. Asks Claude (Haiku, for speed/cost) to look at the actual columns of
// the chosen group's tables and propose 3–6 filter criteria that would be
// genuinely useful when migrating this specific data — date-range filters
// when temporal columns exist, status filters when boolean/enum columns
// exist, soft-delete exclusion when deleted_at-style columns exist, and so
// on. Each suggestion comes with a one-line reason citing the columns it
// was derived from, so the UI can render "smart-looking" chips.
//
// Body:
//   {
//     "groupId":   "financial",
//     "groupName": "Financial Data",
//     "groupDesc": "Invoices, payments, ledger entries...",
//     "tables":    [
//       { "name": "Invoices", "columns": [{ "name": "invoice_id", "dataType": "int" }, ...] },
//       ...
//     ]
//   }
//
// Response:
//   {
//     "suggestions": [
//       {
//         "id": "date-range",
//         "label": "Date range",
//         "type": "date-range",                    // date-range | boolean | enum | text
//         "reason": "Most tables have invoice_date and paid_date columns",
//         "evidence": ["invoice_date in 87% of tables"],
//         "default": { "from": "2 years ago", "to": "today" },
//         "predicate": "invoice_date >= :from AND invoice_date <= :to"
//       },
//       ...
//     ]
//   }

const { app } = require('@azure/functions');

// ── CORS (matches existing functions) ────────────────────────────────────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-user-id',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type':                 'application/json'
};

const ok  = (body)      => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const err = (code, msg) => ({ status: code, headers: CORS, body: JSON.stringify({ error: msg }) });

// ── System prompt for the suggester ─────────────────────────────────────
// Asks Haiku to act as a migration analyst — look at column metadata and
// suggest filter criteria that meaningfully reduce volume or risk. The
// prompt is tuned for the legal/conversion domain (date-bounded data,
// status flags, soft deletes, PII handling), but the rules generalise.
const SYSTEM_PROMPT = `You are a migration data analyst helping a non-technical user choose criteria for moving a group of tables from a source database to a target.

You will be given a group of tables and a sample of column metadata (column names + data types). Your job is to suggest 3 to 6 filter criteria that would be genuinely useful for this specific data — based on what columns actually exist.

ALWAYS consider these dimensions if the schema supports them:
- Date range filtering when columns like *_date, *_at, created/modified/closed/posted/transaction dates exist
- Status filtering when columns like status, state, type, void, posted, active, closed exist (boolean or short string enums)
- Soft-delete exclusion when columns like deleted_at, is_deleted, archived_at, void exist
- Open/closed records when paired columns like open_date + close_date exist (suggest "only open" or "only closed")
- PII handling when columns like email, phone, address, name, ssn, password, dob exist

DO NOT suggest:
- Generic "name not empty" / "id is not null" filters — these are noise
- Filters on columns you're not certain exist (no guessing)
- More than 6 criteria — pick the 3–6 most impactful

RESPONSE FORMAT — return ONLY valid JSON, no preamble, no markdown:
{
  "suggestions": [
    {
      "id": "date-range",
      "label": "Short human label (3–5 words)",
      "type": "date-range" | "boolean" | "enum" | "text",
      "reason": "One short sentence explaining why this is relevant, citing specific column names",
      "evidence": ["short fragment naming the columns this is based on"],
      "default": <type-appropriate default>,
      "predicate": "rough SQL-like WHERE fragment using :placeholders for variables"
    }
  ]
}

DEFAULT VALUE FORMATS by type:
- date-range:  { "from": "2 years ago", "to": "today" }   (use plain English; the UI converts to dates)
- boolean:     true | false
- enum:        { "options": ["voided","posted","draft"], "selected": ["posted"] }
- text:        ""  (empty)

Never invent column names. If a column you'd want isn't in the input, omit that suggestion.`;

// ── Helpers ─────────────────────────────────────────────────────────────
// Compress the input to keep the prompt small. Per table, pass the name and
// a deduplicated list of column-name + data-type pairs. Cap to ~50 sample
// tables to keep tokens bounded for groups with thousands of tables.
function compressTablesForPrompt(tables) {
  const cap = Math.min(tables.length, 50);
  return tables.slice(0, cap).map(t => ({
    name: t.name,
    columns: (t.columns || []).slice(0, 30).map(c => `${c.name}:${c.dataType || 'unknown'}`)
  }));
}

// ── Route registration (v4 programming model) ───────────────────────────
app.http('agent-suggest-criteria', {
  methods:   ['POST', 'OPTIONS'],
  authLevel: 'function',
  route:     'agent/suggest-criteria',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 204, headers: CORS, body: '' };

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) {
      ctx.log.error('ANTHROPIC_API_KEY not configured');
      return err(500, 'Server not configured — ANTHROPIC_API_KEY missing');
    }

    const body = await req.json().catch(() => null);
    if (!body || typeof body !== 'object') return err(400, 'Invalid JSON body');

    const groupId   = (body.groupId   || '').toString();
    const groupName = (body.groupName || 'Selected group').toString();
    const groupDesc = (body.groupDesc || '').toString();
    const tables    = Array.isArray(body.tables) ? body.tables : [];

    if (tables.length === 0) {
      return err(400, 'tables array is required and must be non-empty');
    }

    const compressed = compressTablesForPrompt(tables);
    const totalColCount = compressed.reduce((s, t) => s + t.columns.length, 0);

    const userMsg = `Group: ${groupName}
Description: ${groupDesc || '(none)'}
Total tables in group: ${tables.length} (showing first ${compressed.length})
Total columns sampled: ${totalColCount}

Tables and columns:
${compressed.map(t => `- ${t.name}: [${t.columns.join(', ')}]`).join('\n')}

Suggest 3–6 filter criteria for migrating this group. Return JSON only.`;

    ctx.log(`[suggest-criteria] group=${groupId} tables=${tables.length} sampled=${compressed.length} cols=${totalColCount}`);

    let resp;
    try {
      resp = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type':      'application/json',
          'x-api-key':         apiKey,
          'anthropic-version': '2023-06-01'
        },
        body: JSON.stringify({
          // Haiku 4.5 — fast and cheap, plenty smart enough for structured
          // suggestion of well-bounded length.
          model:      'claude-haiku-4-5-20251001',
          max_tokens: 1200,
          system:     SYSTEM_PROMPT,
          messages: [{ role: 'user', content: userMsg }]
        }),
        // 30s ceiling — Haiku usually responds in <2s, but cold paths happen.
        signal: AbortSignal.timeout(30000)
      });
    } catch (e) {
      ctx.log.error('Anthropic fetch failed:', e.message);
      return err(502, `Upstream fetch failed: ${e.message}`);
    }

    if (!resp.ok) {
      const errText = await resp.text().catch(() => '');
      ctx.log.error('Anthropic API error', resp.status, errText.slice(0, 400));
      return err(502, `Upstream error (${resp.status})`);
    }

    let data;
    try { data = await resp.json(); }
    catch (e) { return err(502, `Could not parse Anthropic response: ${e.message}`); }

    // Extract the text content and parse the embedded JSON.
    const rawText = (data.content || [])
      .filter(b => b.type === 'text')
      .map(b => b.text)
      .join('')
      .trim();

    if (!rawText) {
      ctx.log.error('Empty content in Anthropic response');
      return err(502, 'Model returned empty content');
    }

    // Sometimes the model wraps JSON in ```json fences despite the prompt.
    // Strip them defensively before parsing.
    const stripped = rawText
      .replace(/^```(?:json)?\s*/i, '')
      .replace(/\s*```$/i, '')
      .trim();

    let parsed;
    try {
      parsed = JSON.parse(stripped);
    } catch (e) {
      ctx.log.error('Could not parse model JSON:', e.message, 'Raw:', stripped.slice(0, 300));
      return err(502, 'Model returned non-JSON content');
    }

    const suggestions = Array.isArray(parsed.suggestions) ? parsed.suggestions : [];

    // Light validation — drop entries missing required fields, cap to 6,
    // ensure each has a usable shape so the frontend doesn't have to defend
    // against missing properties.
    const validTypes = new Set(['date-range', 'boolean', 'enum', 'text']);
    const cleaned = [];
    for (const s of suggestions) {
      if (!s || typeof s !== 'object') continue;
      if (!s.label || !s.type || !validTypes.has(s.type)) continue;
      cleaned.push({
        id:        (s.id || s.label).toString().toLowerCase().replace(/[^a-z0-9]+/g, '-').slice(0, 40),
        label:     String(s.label).slice(0, 60),
        type:      s.type,
        reason:    (s.reason || '').toString().slice(0, 200),
        evidence:  Array.isArray(s.evidence) ? s.evidence.slice(0, 4).map(e => String(e).slice(0, 100)) : [],
        default:   s.default ?? null,
        predicate: (s.predicate || '').toString().slice(0, 300)
      });
      if (cleaned.length >= 6) break;
    }

    return ok({ suggestions: cleaned });
  }
});
