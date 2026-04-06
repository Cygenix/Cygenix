// netlify/functions/projects.js
const { getStore } = require('@netlify/blobs');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Content-Type': 'application/json',
};

function ok(data)            { return { statusCode: 200,  headers: CORS, body: JSON.stringify(data) }; }
function fail(msg, code=500) { return { statusCode: code, headers: CORS, body: JSON.stringify({ error: msg }) }; }

function decodeJWT(token) {
  try {
    const payload = Buffer.from(token.split('.')[1], 'base64').toString('utf8');
    return JSON.parse(payload);
  } catch (e) { throw new Error('Could not decode token: ' + e.message); }
}

function safeStoreName(userId) {
  return 'proj-' + userId.replace(/[^a-zA-Z0-9]/g, '').slice(0, 48);
}

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };

  const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
  const token  = process.env.NETLIFY_API_TOKEN;

  if (!siteID) return fail('NETLIFY_SITE_ID not set');
  if (!token)  return fail('NETLIFY_API_TOKEN not set');

  const authHeader = event.headers.authorization || event.headers.Authorization || '';
  const jwtToken = authHeader.replace(/^Bearer\s+/i, '').trim();
  if (!jwtToken) return fail('Authorization header missing', 401);

  let userId, userEmail;
  try {
    const decoded = decodeJWT(jwtToken);
    if (!decoded.sub) throw new Error('No user ID in token');
    if (decoded.exp && decoded.exp < Math.floor(Date.now() / 1000)) return fail('Session expired', 401);
    userId    = decoded.sub;
    userEmail = decoded.email || '';
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
      const projects = [];
      for (const blob of blobs) {
        try {
          const data = await store.get(blob.key, { type: 'json' });
          if (data) projects.push({
            id: blob.key, name: data.name || 'Untitled',
            status: data.status || 'in-progress', updatedAt: data.updatedAt || '',
            sourceFile: data.sourceFile || null, targetDb: data.targetDb || null,
            totalRows: data.totalRows || 0,
          });
        } catch {}
      }
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
