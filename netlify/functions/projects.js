// netlify/functions/projects.js
// Saves and loads user projects using Netlify Blobs.
// Decodes the GoTrue JWT to get user ID without needing jsonwebtoken package.

const { getStore } = require('@netlify/blobs');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Content-Type': 'application/json',
};

// Decode a JWT without verifying signature — we trust Netlify Identity issued it
function decodeJWT(token) {
  try {
    const parts = token.split('.');
    if (parts.length !== 3) throw new Error('Not a JWT');
    const payload = Buffer.from(parts[1], 'base64url').toString('utf8');
    return JSON.parse(payload);
  } catch (e) {
    throw new Error('Invalid token: ' + e.message);
  }
}

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };

  // Extract and decode JWT
  const authHeader = event.headers.authorization || event.headers.Authorization || '';
  const token = authHeader.replace(/^Bearer\s+/i, '').trim();
  if (!token) {
    return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'No token provided' }) };
  }

  let userId, userEmail;
  try {
    const decoded = decodeJWT(token);
    userId    = decoded.sub;
    userEmail = decoded.email || '';
    if (!userId) throw new Error('No user ID in token');

    // Check token is not expired
    if (decoded.exp && decoded.exp < Math.floor(Date.now() / 1000)) {
      return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Token expired — please sign in again' }) };
    }
  } catch (e) {
    return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: 'Invalid token: ' + e.message }) };
  }

  // Use Netlify Blobs with a store name scoped to this user
  let store;
  try {
    store = getStore('cygenix-projects-' + userId.replace(/[^a-zA-Z0-9]/g, ''));
  } catch (e) {
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: 'Could not initialise storage: ' + e.message }) };
  }

  const method    = event.httpMethod;
  const projectId = event.queryStringParameters?.id;

  let body = {};
  try { body = JSON.parse(event.body || '{}'); } catch {}

  try {

    // ── LIST all projects ──────────────────────────────────────────────────────
    if (method === 'GET' && !projectId) {
      const { blobs } = await store.list();
      const projects = [];
      for (const blob of blobs) {
        try {
          const raw = await store.get(blob.key, { type: 'json' });
          if (raw) projects.push({
            id:         blob.key,
            name:       raw.name       || blob.key,
            status:     raw.status     || 'in-progress',
            updatedAt:  raw.updatedAt  || '',
            sourceFile: raw.sourceFile || null,
            targetDb:   raw.targetDb   || null,
            totalRows:  raw.totalRows  || 0,
          });
        } catch { /* skip corrupted entries */ }
      }
      projects.sort((a, b) => new Date(b.updatedAt) - new Date(a.updatedAt));
      return { statusCode: 200, headers: CORS, body: JSON.stringify({ projects }) };
    }

    // ── GET one project ────────────────────────────────────────────────────────
    if (method === 'GET' && projectId) {
      const project = await store.get(projectId, { type: 'json' });
      if (!project) return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'Project not found' }) };
      return { statusCode: 200, headers: CORS, body: JSON.stringify({ project }) };
    }

    // ── CREATE or UPDATE ───────────────────────────────────────────────────────
    if (method === 'POST' || method === 'PUT') {
      const { project, name } = body;
      if (!project) return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'project data required' }) };

      const id = projectId || body.id || ('proj_' + Date.now());
      const toSave = {
        ...project,
        id,
        name:      name || project.name || 'Untitled Project',
        updatedAt: new Date().toISOString(),
        userEmail,
        userId,
      };

      await store.setJSON(id, toSave);
      return { statusCode: 200, headers: CORS, body: JSON.stringify({ id, saved: true }) };
    }

    // ── DELETE ─────────────────────────────────────────────────────────────────
    if (method === 'DELETE' && projectId) {
      await store.delete(projectId);
      return { statusCode: 200, headers: CORS, body: JSON.stringify({ deleted: true }) };
    }

    return { statusCode: 405, headers: CORS, body: JSON.stringify({ error: 'Method not allowed' }) };

  } catch (e) {
    console.error('Projects function error:', e);
    return {
      statusCode: 500,
      headers: CORS,
      body: JSON.stringify({
        error: e.message,
        hint: e.message.includes('Blobs') || e.message.includes('store')
          ? 'Netlify Blobs may not be enabled. Go to Netlify → your site → Storage → Enable Blobs.'
          : null
      })
    };
  }
};
