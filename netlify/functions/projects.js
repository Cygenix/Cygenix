// netlify/functions/projects.js
const { getStore } = require('@netlify/blobs');
const { verifyAuthHeader } = require('./lib/entra-auth');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Content-Type': 'application/json',
};

function ok(data)            { return { statusCode: 200,  headers: CORS, body: JSON.stringify(data) }; }
function fail(msg, code=500) { return { statusCode: code, headers: CORS, body: JSON.stringify({ error: msg }) }; }

function safeStoreName(userId) {
  return 'proj-' + userId.replace(/[^a-zA-Z0-9]/g, '').slice(0, 48);
}

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };

  const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
  const token  = process.env.NETLIFY_API_TOKEN;

  if (!siteID) return fail('NETLIFY_SITE_ID not set');
  if (!token)  return fail('NETLIFY_API_TOKEN not set');

  // Verify the token signature against Entra's JWKS — identity comes from
  // verified claims only. The store is keyed by the token's `sub`, the same
  // claim the old (unverified) decode used, so existing stores still match.
  let userId, userEmail;
  try {
    const authed = await verifyAuthHeader(event);
    if (!authed.sub) throw new Error('No user ID in token');
    userId    = authed.sub;
    userEmail = authed.email;
  } catch (e) {
    return fail('Auth error: ' + e.message, 401);
  }

  let store;
  try {
    store = getStore({ name: safeStoreName(userId), siteID, token });
  } catch (e) {
    return fail('Storage init failed: ' + e.message);
  }

  const method    = event.httpMethod;
  const projectId = event.queryStringParameters?.id;
  let body = {};
  try { body = JSON.parse(event.body || '{}'); } catch {}

  try {
    if (method === 'GET' && !projectId) {
      const { blobs } = await store.list();
      // Fetch summaries in parallel — the previous serial loop paid one
      // round-trip per project and could hit the function timeout at ~100.
      const projects = (await Promise.all(blobs.map(async (blob) => {
        try {
          const data = await store.get(blob.key, { type: 'json' });
          if (!data) return null;
          return {
            id: blob.key, name: data.name || 'Untitled',
            status: data.status || 'in-progress', updatedAt: data.updatedAt || '',
            sourceFile: data.sourceFile || null, targetDb: data.targetDb || null,
            totalRows: data.totalRows || 0,
          };
        } catch { return null; }
      }))).filter(Boolean);
      projects.sort((a, b) => new Date(b.updatedAt) - new Date(a.updatedAt));
      return ok({ projects });
    }

    if (method === 'GET' && projectId) {
      const data = await store.get(projectId, { type: 'json' });
      if (!data) return fail('Project not found', 404);
      return ok({ project: data });
    }

    if (method === 'POST') {
      const { project, name } = body;
      const id = 'proj_' + Date.now();
      const toSave = {
        ...(project || {}), id,
        name: name || project?.name || 'Untitled Project',
        status: project?.status || 'in-progress',
        updatedAt: new Date().toISOString(),
        createdAt: new Date().toISOString(),
        userEmail, userId,
      };
      await store.setJSON(id, toSave);
      return ok({ id, saved: true });
    }

    if (method === 'PUT' && projectId) {
      const { project, name } = body;
      if (!project) return fail('project data required', 400);
      const existing = await store.get(projectId, { type: 'json' }) || {};
      await store.setJSON(projectId, {
        ...existing, ...project, id: projectId,
        name: name || project.name || existing.name || 'Untitled',
        updatedAt: new Date().toISOString(), userEmail, userId,
      });
      return ok({ id: projectId, saved: true });
    }

    if (method === 'DELETE' && projectId) {
      await store.delete(projectId);
      return ok({ deleted: true });
    }

    return fail('Method not allowed', 405);
  } catch (e) {
    console.error('[projects]', e.message);
    return fail('Operation failed: ' + e.message);
  }
};
