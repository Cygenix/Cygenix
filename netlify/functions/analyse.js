// netlify/functions/analyse.js
// Reads uploaded database export file content and uses Claude to:
// 1. Suggest a SQL Server schema (CREATE TABLE statements)
// 2. Generate migration SQL (INSERT statements or conversion scripts)

exports.handler = async function (event) {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
  if (!ANTHROPIC_API_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'API key not configured. Add ANTHROPIC_API_KEY in Netlify environment variables.' })
    };
  }

  let body;
  try {
    body = JSON.parse(event.body);
  } catch {
    return { statusCode: 400, body: JSON.stringify({ error: 'Invalid JSON body' }) };
  }

  const { files, jobName, targetServer } = body;

  if (!files || !Array.isArray(files) || files.length === 0) {
    return { statusCode: 400, body: JSON.stringify({ error: 'No files provided' }) };
  }

  // Build a detailed description of each file including any content snippet
  const fileDescriptions = files.map(f => {
    let desc = `FILE: ${f.name}\n  Size: ${formatSize(f.size)}\n  Type: ${f.type || getExt(f.name)}`;
    if (f.contentSnippet) {
      desc += `\n  Content preview (first 3000 chars):\n${f.contentSnippet.slice(0, 3000)}`;
    }
    return desc;
  }).join('\n\n');

  const target = targetServer || 'Microsoft SQL Server (on-premises)';

  const prompt = `You are Cygenix, an expert database migration assistant specialising in ${target}. 

A user wants to migrate the following database export file(s) into ${target}.

${fileDescriptions}

Your task is to analyse the file(s) and produce a complete, professional migration package. Structure your response with these exact sections:

---
## 1. SOURCE ANALYSIS
Describe what you found in the source file(s): database type, tables detected, estimated row counts, data types used, relationships/foreign keys, indexes, and any notable quirks or issues.

---
## 2. SQL SERVER SCHEMA
Provide complete, executable SQL Server CREATE TABLE statements for all detected tables. Use appropriate SQL Server data types (e.g. NVARCHAR, INT, DATETIME2, DECIMAL, BIT). Include:
- Primary keys
- Foreign key constraints
- Indexes on likely query columns
- NULL/NOT NULL constraints
- Default values where appropriate

Format as clean, commented SQL code blocks.

---
## 3. MIGRATION SQL
Provide the migration SQL to move the data into SQL Server. Depending on the source:
- For .sql dumps: provide adapted INSERT statements with SQL Server syntax
- For .mdb (Access): provide OPENROWSET or linked server queries, or equivalent INSERT/SELECT
- For .bak: provide RESTORE DATABASE commands and any schema adjustments needed
Include any necessary USE, GO, SET IDENTITY_INSERT statements.

---
## 4. DATA TYPE MAPPING
A clear table showing how source data types map to SQL Server equivalents.

---
## 5. MIGRATION CHECKLIST
Step-by-step instructions for an IT team to execute this migration on an on-premises SQL Server instance, including:
- Prerequisites (SQL Server version, permissions needed)
- Order of operations
- How to verify row counts after migration
- Rollback steps if something goes wrong

---
## 6. RISK FLAGS
Any data quality issues, encoding problems, reserved keyword conflicts, or compatibility warnings the team should know about before running the migration.

Be specific and technical. Generate real, runnable SQL — not placeholders.`;

  try {
    const response = await fetch('https://api.anthropic.com/v1/messages', {
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

    if (!response.ok) {
      const err = await response.text();
      return { statusCode: response.status, body: JSON.stringify({ error: 'Anthropic API error', detail: err }) };
    }

    const data = await response.json();
    const analysis = data.content?.map(b => b.text || '').join('') || 'No analysis returned.';

    // Extract the schema SQL and migration SQL as separate fields for the UI
    const schemaMatch = analysis.match(/## 2\. SQL SERVER SCHEMA([\s\S]*?)(?=## 3\.)/i);
    const migrationMatch = analysis.match(/## 3\. MIGRATION SQL([\s\S]*?)(?=## 4\.)/i);

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        analysis,
        schemaSQL: schemaMatch ? schemaMatch[1].trim() : null,
        migrationSQL: migrationMatch ? migrationMatch[1].trim() : null,
      }),
    };

  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Server error', detail: err.message })
    };
  }
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
