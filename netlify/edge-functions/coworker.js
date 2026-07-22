// netlify/edge-functions/coworker.js
// Conversational "Co-Worker" endpoint for Cygenix.
// Runs on Deno at Netlify's edge (no timeout limit) — same pattern as analyse.js.
//
// Accepts a running conversation plus lightweight workspace context and returns
// the assistant's next reply. The Co-Worker helps the user develop migration
// scripts, edit documents and customise conversion logic. It is deliberately a
// thin proxy to Claude: all the UI/state lives in /public/coworker.html.

export default async function handler(request, context) {
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
  };

  if (request.method === 'OPTIONS') {
    return new Response('', { status: 200, headers: corsHeaders });
  }
  if (request.method !== 'POST') {
    return json({ error: 'Method not allowed' }, 405, corsHeaders);
  }

  const ANTHROPIC_API_KEY = Deno.env.get('ANTHROPIC_API_KEY');
  if (!ANTHROPIC_API_KEY) {
    return json({ error: 'ANTHROPIC_API_KEY is not set in Netlify environment variables.' }, 500, corsHeaders);
  }

  let body;
  try {
    body = await request.json();
  } catch (e) {
    return json({ error: 'Invalid JSON: ' + e.message }, 400, corsHeaders);
  }

  const { messages, context: ctx } = body;
  if (!Array.isArray(messages) || messages.length === 0) {
    return json({ error: 'No messages provided.' }, 400, corsHeaders);
  }

  // Keep only the trailing window of turns so the request stays bounded.
  const trimmed = messages
    .filter(m => m && (m.role === 'user' || m.role === 'assistant') && typeof m.content === 'string')
    .slice(-24)
    .map(m => ({ role: m.role, content: m.content.slice(0, 24000) }));

  const system = buildSystem(ctx || {});

  try {
    const claudeRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 4096,
        system,
        messages: trimmed,
      }),
    });

    const rawText = await claudeRes.text();
    if (!claudeRes.ok) {
      let detail = rawText;
      try { detail = JSON.parse(rawText).error?.message || rawText; } catch {}
      return json({ error: `Anthropic API error (${claudeRes.status}): ${detail}` }, claudeRes.status, corsHeaders);
    }

    const data = JSON.parse(rawText);
    const reply = data.content?.map(b => b.text || '').join('') || 'No response returned.';
    return json({ reply }, 200, corsHeaders);
  } catch (err) {
    return json({ error: 'Edge function error: ' + err.message }, 500, corsHeaders);
  }
}

function buildSystem(ctx) {
  const lines = [];
  lines.push(
    'You are the Cygenix Co-Worker — an AI teammate embedded in the Cygenix SQL database migration platform.',
    'You help the user in three ways: (1) develop and refine migration scripts (SQL, and glue code), (2) draft and edit documents (runbooks, mapping specs, summaries, handover notes), and (3) customise conversion logic (column mappings, transformations, validation rules, Was/Is value translations).',
    '',
    'Guidelines:',
    '- Be concise and practical. Lead with the answer or the artifact, then a short explanation.',
    '- When you produce a script or document the user can use, put the FULL artifact in a single fenced code block. Tag SQL blocks as ```sql, documents as ```markdown, other code with its language. The UI lets the user apply a fenced block straight into their workspace, so make each block self-contained and runnable/complete.',
    '- Prefer real, runnable SQL for Microsoft SQL Server / Azure SQL unless the user specifies another target. No placeholders like "-- your logic here".',
    '- Ask a clarifying question only when you genuinely cannot proceed; otherwise make a sensible assumption and state it briefly.',
    '- You cannot execute anything or reach the database yourself — you produce artifacts the user runs inside Cygenix (SQL Editor, Object Mapping, Validation, etc.). Point them to the relevant Cygenix screen when helpful.',
  );

  const p = ctx.project;
  if (p && (p.name || p.client)) {
    lines.push('', 'Active project context:');
    if (p.name) lines.push(`- Project: ${String(p.name).slice(0, 120)}`);
    if (p.client) lines.push(`- Client: ${String(p.client).slice(0, 120)}`);
    if (p.source) lines.push(`- Source system: ${String(p.source).slice(0, 120)}`);
    if (p.target) lines.push(`- Target system: ${String(p.target).slice(0, 120)}`);
  }
  if (Array.isArray(ctx.connections) && ctx.connections.length) {
    lines.push('', 'Configured connections: ' + ctx.connections.map(c => String(c).slice(0, 60)).join(', '));
  }
  if (ctx.artifactType && ctx.artifactContent && String(ctx.artifactContent).trim()) {
    lines.push(
      '',
      `The user currently has this ${ctx.artifactType} open in their workspace. When they ask you to edit/refine "the script" or "the document", modify THIS content and return the full updated version in one fenced block:`,
      '--- BEGIN CURRENT ' + String(ctx.artifactType).toUpperCase() + ' ---',
      String(ctx.artifactContent).slice(0, 16000),
      '--- END CURRENT ' + String(ctx.artifactType).toUpperCase() + ' ---',
    );
  }

  // The virtual Drive is the co-worker's workspace / development location.
  const d = ctx.drive;
  if (d && ((d.fileCount || 0) > 0 || (Array.isArray(d.folders) && d.folders.length))) {
    lines.push(
      '',
      'The user has a virtual Drive that is YOUR workspace and development location. The files below are the project artifacts you can build on and refer to by path. (You only see a file\'s contents once the user opens it into the workspace — but you always know these paths exist.)'
    );
    const folders = (d.folders || []).slice(0, 60);
    if (folders.length) lines.push('- Folders: ' + folders.map(f => String(f).slice(0, 120)).join(', '));
    const files = (d.files || []).slice(0, 120);
    if (files.length) lines.push('- Files: ' + files.map(f => String(f && f.path).slice(0, 120)).join(', '));
    if (d.openFile) lines.push(`- Currently open in the workspace (from the Drive): ${String(d.openFile).slice(0, 160)}`);
    lines.push('When you produce a file the user should keep, tell them they can save it to the Drive (the "🗂 To Drive" button), and suggest a sensible Drive folder/path to organise it.');
  }

  return lines.join('\n');
}

function json(obj, status, corsHeaders) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { ...corsHeaders, 'Content-Type': 'application/json' },
  });
}
