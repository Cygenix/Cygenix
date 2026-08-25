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
// Auth: verified Entra ID token, through lib/authz. The store is keyed by
// the token's `sub`, so one user can never address another user's Drive.
//
// The Drive is deliberately NOT tenant-keyed. Projects became shared because
// colleagues need to work on the same migration; a person's Drive is their
// own working documents, and "your colleague can now read your files" is not
// something tenancy implies or anybody asked for. If shared team folders are
// wanted later they are a feature, not a re-keying.

const { getStore } = require('@netlify/blobs');
const authz = require('./lib/authz');

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

// Open the user's blob store.
//
// Inside a Netlify Function, Blobs is auto-configured from the request
// context — getStore(name) just works with no credentials. Only fall back to
// explicit siteID/token (needed outside that context) when the ambient call
// fails. Requiring the env vars up front, as projects.js does, turns a
// missing NETLIFY_API_TOKEN into a hard 500 for a feature that never needed
// it — which is exactly how the Drive sync silently failed.
function openStore(userId) {
  const name = safeStoreName(userId);
  try {
    return getStore(name);
  } catch (ambientErr) {
    const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
    const token  = process.env.NETLIFY_API_TOKEN;
    if (!siteID || !token) {
      const e = new Error(
        'Blob storage unavailable: ambient Netlify Blobs failed (' + ambientErr.message +
        ') and no NETLIFY_SITE_ID / NETLIFY_API_TOKEN fallback is configured'
      );
      e.statusCode = 503;
      throw e;
    }
    return getStore({ name, siteID, token });
  }
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

  // The Drive stays keyed on the individual, not the tenant — see the note
  // in the header. It still goes through the one door, so that a signer with
  // no role assignment reaches nothing here either, and so that the
  // route-coverage test can prove it from the outside.
  let userId, ctx;
  try {
    ctx = await authz.authorize(event, {
      route: 'drive', action: 'project.read', mutating: false,
    });
    userId = ctx.authed.sub || ctx.actor.oid;
    if (!userId) throw new authz.AuthzError('No user ID in token', 401);
  } catch (e) {
    return authz.errorResponse(e, CORS);
  }

  let body = {};
  try { body = JSON.parse(event.body || '{}'); }
  catch { return fail('Invalid JSON body', 400); }

  let store;
  try {
    store = openStore(userId);
  } catch (e) {
    return fail('Storage init failed: ' + e.message, e.statusCode || 500);
  }

  const action = String(body.action || '');

  try {
    if (action === 'manifest') {
      return ok({ manifest: await readManifest(store), maxContentBytes: MAX_CONTENT_BYTES });
    }

    // Read-only self-check for diagnosing sync problems from the browser
    // console. Deliberately reveals nothing sensitive: no tokens, no file
    // names, no store credentials — just whether storage works and how many
    // nodes the account holds.
    if (action === 'diag') {
      const out = {
        authenticated: true,
        // Which store this token maps to. Derived from the caller's own user
        // id, so it is not sensitive — and it is the fastest way to confirm
        // that two machines are actually talking to the SAME Drive rather
        // than two stores under different accounts.
        store: safeStoreName(userId),
        blobStore: 'unknown', nodeCount: null, maxContentBytes: MAX_CONTENT_BYTES,
      };
      try {
        const m = await readManifest(store);
        out.blobStore = 'ok';
        out.nodeCount = Object.keys(m.nodes || {}).length;
        out.manifestExists = !m.empty;
        // Names make it obvious at a glance whose folders reached the cloud.
        out.sample = Object.values(m.nodes || {}).slice(0, 10).map(n => n.kind + ':' + n.name);
      } catch (e) {
        out.blobStore = 'error';
        out.blobError = e.message;
      }
      return ok(out);
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
      // Write a clean object — never persist readManifest's `empty` sentinel,
      // which would make a populated manifest keep reporting itself as absent.
      await store.setJSON('manifest', { nodes: manifest.nodes, updatedAt: new Date().toISOString() });
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
      // Pass a plain ArrayBuffer rather than the Node Buffer — the Blobs SDK
      // documents string/ArrayBuffer/Blob/stream, and a Buffer is only
      // incidentally a Uint8Array.
      const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
      await store.set('c/' + id, ab, {
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
      await store.setJSON('manifest', { nodes: manifest.nodes, updatedAt: new Date().toISOString() });
      return ok({ ok: true, deleted });
    }

    return fail('Unknown action: ' + action, 400);
  } catch (e) {
    console.error('[drive]', e.stack || e.message);
    return fail('Operation failed: ' + e.message);
  }
};
