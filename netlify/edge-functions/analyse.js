// netlify/edge-functions/analyse.js
// Edge functions run on Deno at Netlify's edge network.
// They have NO timeout limit on the free plan — perfect for Claude API calls.
// Uses ES module syntax (not CommonJS).

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
    return new Response(JSON.stringify({ error: 'Method not allowed' }), {
      status: 405,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  const ANTHROPIC_API_KEY = Deno.env.get('ANTHROPIC_API_KEY');
  if (!ANTHROPIC_API_KEY) {
    return new Response(JSON.stringify({ error: 'ANTHROPIC_API_KEY is not set in Netlify environment variables.' }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  let body;
  try {
    body = await request.json();
  } catch (e) {
    return new Response(JSON.stringify({ error: 'Invalid JSON: ' + e.message }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  const { files, jobName, targetServer, sourceSystem } = body;

  if (!files || !Array.isArray(files) || files.length === 0) {
    return new Response(JSON.stringify({ error: 'No files provided.' }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  const fileDescriptions = files.map(f => {
    let desc = `FILE: ${f.name}\n  Size: ${formatSize(f.size)}\n  Type: ${f.type || getExt(f.name)}`;
    if (f.contentSnippet && f.contentSnippet.trim().length > 0) {
      desc += `\n  Content preview:\n${f.contentSnippet.trim().slice(0, 2000)}`;
    } else {
      desc += `\n  (Binary file — use filename and type for schema guidance)`;
    }
    return desc;
  }).join('\n\n---\n\n');

  const target = targetServer || 'Microsoft SQL Server (on-premises)';
  const source = sourceSystem ? `Source system: ${sourceSystem}\n` : '';

  const prompt = `You are Cygenix, an expert database migration assistant specialising in ${target}.

A user wants to migrate the following database export file(s) into ${target}.
${source}
${fileDescriptions}

Produce a complete SQL Server migration package with these exact sections:

## 1. SOURCE ANALYSIS
Describe what you found: database type, tables detected, estimated row counts, data types, relationships, indexes, and any quirks.

## 2. SQL SERVER SCHEMA
Complete executable SQL Server CREATE TABLE statements. Use SQL Server types (NVARCHAR, INT, DATETIME2, DECIMAL, BIT etc). Include primary keys, foreign keys, indexes, NULL constraints, defaults. Use proper SQL comments.

## 3. MIGRATION SQL
The actual migration SQL for SQL Server. For .sql dumps: adapted INSERT statements. For .mdb/Access: OPENROWSET or SSMA guidance. For .bak: RESTORE commands and schema adjustments. Include USE, GO, SET IDENTITY_INSERT where needed.

## 4. DATA TYPE MAPPING
Table showing source types mapped to SQL Server equivalents.

## 5. MIGRATION CHECKLIST
Step-by-step for an IT team including prerequisites, order of operations, row count verification, and rollback steps.

## 6. RISK FLAGS
Data quality issues, encoding problems, reserved keywords, or compatibility warnings.

Generate real runnable SQL — not placeholders.`;

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
        messages: [{ role: 'user', content: prompt }],
      }),
    });

    const rawText = await claudeRes.text();

    if (!claudeRes.ok) {
      let detail = rawText;
      try { detail = JSON.parse(rawText).error?.message || rawText; } catch {}
      return new Response(JSON.stringify({ error: `Anthropic API error (${claudeRes.status}): ${detail}` }), {
        status: claudeRes.status,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      });
    }

    const data = JSON.parse(rawText);
    const analysis = data.content?.map(b => b.text || '').join('') || 'No analysis returned.';

    const schemaMatch = analysis.match(/## 2\. SQL SERVER SCHEMA([\s\S]*?)(?=## 3\.)/i);
    const migrationMatch = analysis.match(/## 3\. MIGRATION SQL([\s\S]*?)(?=## 4\.)/i);

    return new Response(JSON.stringify({
      analysis,
      schemaSQL: schemaMatch ? schemaMatch[1].trim() : '',
      migrationSQL: migrationMatch ? migrationMatch[1].trim() : '',
    }), {
      status: 200,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });

  } catch (err) {
    return new Response(JSON.stringify({ error: 'Edge function error: ' + err.message }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }
}

function formatSize(b) {
  if (!b || isNaN(b)) return 'unknown size';
  if (b < 1024) return b + 'B';
  if (b < 1048576) return (b / 1024).toFixed(1) + 'KB';
  return (b / 1048576).toFixed(1) + 'MB';
}

function getExt(name) {
  if (!name) return 'FILE';
  return name.split('.').pop().toUpperCase().slice(0, 4) || 'FILE';
}
