// netlify/functions/analyse.js
// Uses Claude's streaming API and pipes it straight back to the browser.
// This keeps the Netlify function alive the entire time Claude is responding,
// and the browser receives tokens as they arrive — no timeout issues.

exports.handler = async function (event) {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers: { ...headers, 'Content-Type': 'application/json' }, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
  if (!ANTHROPIC_API_KEY) {
    return {
      statusCode: 500,
      headers: { ...headers, 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'ANTHROPIC_API_KEY is not set in Netlify environment variables.' })
    };
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch (e) {
    return { statusCode: 400, headers: { ...headers, 'Content-Type': 'application/json' }, body: JSON.stringify({ error: 'Invalid JSON: ' + e.message }) };
  }

  const { files, jobName, targetServer, sourceSystem } = body;

  if (!files || !Array.isArray(files) || files.length === 0) {
    return { statusCode: 400, headers: { ...headers, 'Content-Type': 'application/json' }, body: JSON.stringify({ error: 'No files provided.' }) };
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

  // Call Claude with streaming enabled
  let claudeResponse;
  try {
    claudeResponse = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 2048,
        stream: false, // Keep non-streaming but reduce tokens to fit in time budget
        messages: [{ role: 'user', content: prompt }],
      }),
    });
  } catch (fetchErr) {
    return {
      statusCode: 502,
      headers: { ...headers, 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'Could not reach Anthropic API: ' + fetchErr.message })
    };
  }

  const rawText = await claudeResponse.text();

  if (!claudeResponse.ok) {
    let detail = rawText;
    try { detail = JSON.parse(rawText).error?.message || rawText; } catch {}
    return {
      statusCode: claudeResponse.status,
      headers: { ...headers, 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: `Anthropic API error (${claudeResponse.status}): ${detail}` })
    };
  }

  let data;
  try {
    data = JSON.parse(rawText);
  } catch (e) {
    return {
      statusCode: 500,
      headers: { ...headers, 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'Failed to parse Anthropic response', detail: rawText.slice(0, 300) })
    };
  }

  const analysis = data.content?.map(b => b.text || '').join('') || 'No analysis returned.';

  const schemaMatch = analysis.match(/## 2\. SQL SERVER SCHEMA([\s\S]*?)(?=## 3\.)/i);
  const migrationMatch = analysis.match(/## 3\. MIGRATION SQL([\s\S]*?)(?=## 4\.)/i);

  return {
    statusCode: 200,
    headers: { ...headers, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      analysis,
      schemaSQL: schemaMatch ? schemaMatch[1].trim() : '',
      migrationSQL: migrationMatch ? migrationMatch[1].trim() : '',
    }),
  };
};

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
