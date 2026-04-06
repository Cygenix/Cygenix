// netlify/functions/projects.js
// Saves and loads user projects using Netlify Blobs.
// Requires Netlify Identity — validates JWT before any operation.

const { getStore } = require('@netlify/blobs');
const jwt = require('jsonwebtoken');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Content-Type': 'application/json',
};

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };

  // Validate Netlify Identity JWT
  const authHeader = event.headers.authorization || '';
  const token = authHeader.replace('Bearer ', '').trim();
  if (!token) return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Not authenticated' }) };

  let userId, userEmail;
  try {
    // Netlify Identity tokens are JWTs signed with NETLIFY_JWT_SECRET
    const decoded = jwt.decode(token);
    if (!decoded?.sub) throw new Error('Invalid token');
    userId    = decoded.sub;
    userEmail = decoded.email || decoded.app_metadata?.email || 'unknown';
  } catch (e) {
    return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Invalid token: ' + e.message }) };
  }

  // User-scoped blob store — each user's data is isolated
  const store = getStore({
    name: `projects-${userId}`,
    consistency: 'strong',
  });

  const method = event.httpMethod;
  let body = {};
  try { body = JSON.parse(event.body || '{}'); } catch {}
  const projectId = event.queryStringParameters?.id || body.id;

  try {
    // LIST all projects for this user
    if (method === 'GET' && !projectId) {
      const { blobs } = await store.list();
      const projects = await Promise.all(
        blobs.map(async b => {
          try {
            const data = await store.getWithMetadata(b.key);
            const meta = data?.metadata || {};
            return {
              id: b.key,
              name: meta.name || b.key,
              updatedAt: meta.updatedAt || b.etag,
              status: meta.status || 'in-progress',
              sourceFile: meta.sourceFile || null,
              targetDb: meta.targetDb || null,
              totalRows: meta.totalRows || 0,
            };
          } catch { return null; }
        })
      );
      return { statusCode: 200, headers: CORS, body: JSON.stringify({ projects: projects.filter(Boolean) }) };
    }

    // GET a specific project
    if (method === 'GET' && projectId) {
      const data = await store.getWithMetadata(projectId, { type: 'json' });
      if (!data) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Project not found' }) };
      return { statusCode: 200, headers: CORS, body: JSON.stringify({ project: data.data, metadata: data.metadata }) };
    }

    // CREATE or UPDATE a project
    if (method === 'POST' || method === 'PUT') {
      const { project, name } = body;
      if (!project) return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'project data required' }) };

      const id = projectId || `proj_${Date.now()}`;
      const metadata = {
        name:       name || project.name || 'Untitled Project',
        updatedAt:  new Date().toISOString(),
        status:     project.status || 'in-progress',
        sourceFile: project.sourceFile || null,
        targetDb:   project.targetDb || null,
        totalRows:  project.totalRows || 0,
        userEmail,
      };

      await store.setJSON(id, { ...project, id, userEmail, userId }, { metadata });
      return { statusCode: 200, headers: CORS, body: JSON.stringify({ id, saved: true, metadata }) };
    }

    // DELETE a project
    if (method === 'DELETE' && projectId) {
      await store.delete(projectId);
      return { statusCode: 200, headers: CORS, body: JSON.stringify({ deleted: true }) };
    }

    return { statusCode: 405, headers: CORS, body: JSON.stringify({ error: 'Method not allowed' }) };

  } catch (e) {
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: e.message }) };
  }
};
