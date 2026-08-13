// netlify/functions/drive.js
//
// Cloud storage for the Co-Worker Drive.
//
// The Drive was previously IndexedDB-only — files lived in one browser on one
// machine and were invisible everywhere else, which defeats the point of
// "uploading" them. This endpoint gives each signed-in user a private
// server-side store so the Drive follows them across machines.
//
// Storage layout (Netlify Blobs, one store per user):
//   'manifest'  → { nodes: { <id>: {id,parentId,name,kind,size,mime,mtime,meta} },
//                   updatedAt }
//   'c/<id>'    → raw file bytes (files only; folders have no content)
//
// Metadata and content are separate blobs on purpose: listing the Drive is one
// small request, and content is fetched only for files that actually changed.
//
// Actions (POST JSON, `action` field):
//   manifest     {}                              → { manifest }
//   put-meta     { nodes: [node, ...] }          → { ok, count }
//   put-content  { id, contentB64, mime }        → { ok, size }
//   get-content  { id }                          → { contentB64, mime }
//   delete       { ids: [id, ...] }              → { ok, deleted }
//
// Auth: verified Entra ID token (shared lib). The store is keyed by the
// token's `sub`, so one user can never address another user's Drive.

const { getStore } = require('@netlify/blobs');
const { verifyAuthHeader } = require('./lib/entra-auth');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

// Netlify's synchronous functions cap the request/response payload at 6 MB,
// and base64 inflates bytes by ~33%. Keep a margin under that. Files above
// this stay local-only; the client reports them rather than failing silently.
const MAX_CONTENT_BYTES = 4 * 1024 * 1024;

function ok(data)            { return { statusCode: 200,  headers: CORS, body: JSON.stringify(data) }; }
function fail(msg, code=500) { return { statusCode: code, headers: CORS, body: JSON.stringify({ error: msg }) }; }

function safeStoreName(userId) {
  return 'drive-' + String(userId).replace(/[^a-zA-Z0-9]/g, '').slice(0, 48);
}

// Only these fields are persisted — never trust a client to set anything else
// into the manifest.
function sanitizeNode(n) {
  if (!n || typeof n !== 'object' || !n.id) return null;
  const out = {
    id:       String(n.id),
    parentId: n.parentId ? String(n.parentId) : '',
    name:     String(n.name || 'untitled').slice(0, 200),
    kind:     n.kind === 'folder' ? 'folder' : 'file',
    mtime:    Number(n.mtime) || Date.now(),
    size:     Number(n.size) || 0,
    mime:     String(n.mime || '').slice(0, 120),
  };
  // meta carries small structured extras (scriptId, jobId, …). Cap it so a
  // client can't stuff the manifest with megabytes of junk.
  if (n.meta && typeof n.meta === 'object') {
    const s = JSON.stringify(n.meta);
    if (s.length <= 4000) out.meta = n.meta;
  }
  return out;
}

// A read FAILURE must never be reported as an empty manifest: the client
// treats "node in baseline but absent from the manifest" as "deleted on
// another machine", so a transient blob-store error returning {} would tell
// every client to delete the user's entire Drive. Missing (new user) is fine
// and returns empty; anything else throws.
async function readManifest(store) {
  const m = await store.get('manifest', { type: 'json' });   // throws on real errors
  if (m && m.nodes && typeof m.nodes === 'object') return m;
  return { nodes: {}, updatedAt: null, empty: true };
}

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };
  if (event.httpMethod !== 'POST')    return fail('Method not allowed', 405);

  const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
  const token  = process.env.NETLIFY_API_TOKEN;
  if (!siteID) return fail('NETLIFY_SITE_ID not set');
  if (!token)  return fail('NETLIFY_API_TOKEN not set');

  let userId;
  try {
    const authed = await verifyAuthHeader(event);
    userId = authed.sub;
    if (!userId) throw new Error('No user ID in token');
  } catch (e) {
    return fail('Auth error: ' + e.message, 401);
  }

  let body = {};
  try { body = JSON.parse(event.body || '{}'); }
  catch { return fail('Invalid JSON body', 400); }

  let store;
  try {
    store = getStore({ name: safeStoreName(userId), siteID, token });
  } catch (e) {
    return fail('Storage init failed: ' + e.message);
  }

  const action = String(body.action || '');

  try {
    if (action === 'manifest') {
      return ok({ manifest: await readManifest(store), maxContentBytes: MAX_CONTENT_BYTES });
    }

    if (action === 'put-meta') {
      const incoming = Array.isArray(body.nodes) ? body.nodes : [];
      if (!incoming.length) return ok({ ok: true, count: 0 });
      if (incoming.length > 2000) return fail('Too many nodes in one request', 400);

      const manifest = await readManifest(store);
      let count = 0;
      for (const raw of incoming) {
        const n = sanitizeNode(raw);
        if (!n) continue;
        manifest.nodes[n.id] = n;
        count++;
      }
      manifest.updatedAt = new Date().toISOString();
      await store.setJSON('manifest', manifest);
      return ok({ ok: true, count });
    }

    if (action === 'put-content') {
      const id = String(body.id || '');
      if (!id) return fail('id required', 400);
      const b64 = String(body.contentB64 || '');
      const buf = Buffer.from(b64, 'base64');
      if (buf.length > MAX_CONTENT_BYTES) {
        return fail(`File exceeds the ${Math.round(MAX_CONTENT_BYTES / 1048576)} MB cloud-sync limit`, 413);
      }
      await store.set('c/' + id, buf, {
        metadata: { mime: String(body.mime || '').slice(0, 120) },
      });
      return ok({ ok: true, size: buf.length });
    }

    if (action === 'get-content') {
      const id = String(body.id || '');
      if (!id) return fail('id required', 400);
      const res = await store.getWithMetadata('c/' + id, { type: 'arrayBuffer' });
      if (!res || !res.data) return fail('Content not found', 404);
      return ok({
        contentB64: Buffer.from(res.data).toString('base64'),
        mime: (res.metadata && res.metadata.mime) || '',
      });
    }

    if (action === 'delete') {
      const ids = Array.isArray(body.ids) ? body.ids.map(String) : [];
      if (!ids.length) return ok({ ok: true, deleted: 0 });

      const manifest = await readManifest(store);
      let deleted = 0;
      for (const id of ids) {
        if (manifest.nodes[id]) { delete manifest.nodes[id]; deleted++; }
        // Content delete is best-effort: a missing blob is not an error, and
        // an orphaned blob is harmless (it's unreachable without a manifest
        // entry) — never fail the whole request over one.
        try { await store.delete('c/' + id); } catch { /* ignore */ }
      }
      manifest.updatedAt = new Date().toISOString();
      await store.setJSON('manifest', manifest);
      return ok({ ok: true, deleted });
    }

    return fail('Unknown action: ' + action, 400);
  } catch (e) {
    console.error('[drive]', e.stack || e.message);
    return fail('Operation failed: ' + e.message);
  }
};
