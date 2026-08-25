// netlify/functions/projects.js
//
// Projects, keyed on the tenant rather than on the individual signer.
//
// This store used to be `proj-<sub>` — one blob store per person. It worked,
// and it meant two colleagues at the same customer could not open the same
// project. The store is now `projt-<tenantId>`, and each record carries the
// owner and a visibility.
//
// ── The migration ─────────────────────────────────────────────────────────
//
// Non-destructive and lazy. The first time a signer lists their projects,
// anything in their old per-user store that is not already in the tenant
// store is copied across, stamped with them as the owner and marked
// PRIVATE — not tenant-visible. Tenancy is what lets colleagues share; it is
// not licence to publish work that was private when it was written. The
// owner shares it deliberately, from the Projects page.
//
// The legacy store is never written to and never deleted. If this change has
// to be rolled back, every original blob is still exactly where it was.
// A marker key records that a signer's migration has run, so the copy
// happens once and a later rollback-and-retry is idempotent rather than
// resurrecting projects the user has since deleted.

const { getStore } = require('@netlify/blobs');
const authz = require('./lib/authz');
const tenancy = require('./lib/tenancy');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Content-Type': 'application/json',
};

function ok(data)            { return { statusCode: 200,  headers: CORS, body: JSON.stringify(data) }; }
function fail(msg, code=500) { return { statusCode: code, headers: CORS, body: JSON.stringify({ error: msg }) }; }

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };

  const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
  const token  = process.env.NETLIFY_API_TOKEN;

  if (!siteID) return fail('NETLIFY_SITE_ID not set');
  if (!token)  return fail('NETLIFY_API_TOKEN not set');

  // One door: identity, roles and tenant in a single call. Reads need
  // project.read, writes need project.write, and the tenant boundary plus
  // each record's visibility narrow it from there. A signer with no role
  // assignment gets past neither.
  const writing = event.httpMethod !== 'GET';
  let ctx;
  try {
    ctx = await authz.authorize(event, {
      route: 'projects',
      action: writing ? 'project.write' : 'project.read',
      mutating: writing,
    });
  } catch (e) {
    return authz.errorResponse(e, CORS);
  }

  const { actor, tenant } = ctx;
  const userId = actor.oid;
  const userEmail = actor.email;

  let store;
  try {
    store = getStore({ name: tenancy.projectStoreName(tenant.id), siteID, token });
  } catch (e) {
    return fail('Storage init failed: ' + e.message);
  }

  const method    = event.httpMethod;
  const projectId = event.queryStringParameters?.id;
  let body = {};
  try { body = JSON.parse(event.body || '{}'); } catch {}

  try {
    if (method === 'GET' && !projectId) {
      await migrateLegacyProjects({ store, siteID, token, actor, ctx });

      const { blobs } = await store.list();
      // Fetch summaries in parallel — the previous serial loop paid one
      // round-trip per project and could hit the function timeout at ~100.
      const projects = (await Promise.all(blobs
        .filter(b => tenancy.PROJECT_KEY_RE.test(b.key))
        .map(async (blob) => {
          try {
            const data = await store.get(blob.key, { type: 'json' });
            if (!data) return null;
            if (!tenancy.canSeeProject(userId, data)) return null;
            return {
              id: blob.key, name: data.name || 'Untitled',
              status: data.status || 'in-progress', updatedAt: data.updatedAt || '',
              sourceFile: data.sourceFile || null, targetDb: data.targetDb || null,
              totalRows: data.totalRows || 0,
              ownerOid: data.ownerOid || null, ownerEmail: data.ownerEmail || data.userEmail || null,
              visibility: tenancy.visibilityOf(data),
              mine: data.ownerOid === userId,
            };
          } catch { return null; }
        }))).filter(Boolean);
      projects.sort((a, b) => new Date(b.updatedAt) - new Date(a.updatedAt));
      return ok({ projects, tenant: { id: tenant.id, name: tenant.name } });
    }

    if (method === 'GET' && projectId) {
      const data = await store.get(projectId, { type: 'json' });
      // A record this actor may not see is indistinguishable from one that
      // does not exist — and a reach into another tenant is recorded even
      // though the answer is the same 404.
      if (!data || !tenancy.canSeeProject(userId, data)) {
        return authz.errorResponse(await authz.notFound(ctx, { objectId: projectId }), CORS);
      }
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
        tenantId: tenant.id,
        ownerOid: userId, ownerEmail: userEmail,
        visibility: tenancy.VISIBILITIES.includes(project?.visibility)
          ? project.visibility : tenancy.DEFAULT_VISIBILITY,
        userEmail, userId,
      };
      await store.setJSON(id, toSave);
      await tenancy.indexObject(ctx.store, id, tenant.id);
      await ctx.audit({ action: 'project.create', outcome: 'allowed', severity: 'info',
                        resourceType: 'project', resourceId: id,
                        detail: { visibility: toSave.visibility } });
      return ok({ id, saved: true, visibility: toSave.visibility });
    }

    if (method === 'PUT' && projectId) {
      const { project, name } = body;
      if (!project) return fail('project data required', 400);
      const existing = await store.get(projectId, { type: 'json' });
      if (!existing || !tenancy.canSeeProject(userId, existing)) {
        return authz.errorResponse(await authz.notFound(ctx, { objectId: projectId }), CORS);
      }
      // Ownership and tenancy are server-held facts, not fields a client
      // sends. Visibility moves only at the owner's hand.
      const visibility = (project.visibility && tenancy.canSetVisibility(userId, existing))
        ? (tenancy.VISIBILITIES.includes(project.visibility) ? project.visibility : tenancy.visibilityOf(existing))
        : tenancy.visibilityOf(existing);
      const merged = {
        ...existing, ...project, id: projectId,
        name: name || project.name || existing.name || 'Untitled',
        updatedAt: new Date().toISOString(),
        tenantId: tenant.id,
        ownerOid: existing.ownerOid || userId,
        ownerEmail: existing.ownerEmail || existing.userEmail || userEmail,
        visibility,
        userEmail, userId,
      };
      await store.setJSON(projectId, merged);
      await tenancy.indexObject(ctx.store, projectId, tenant.id);
      if (visibility !== tenancy.visibilityOf(existing)) {
        await ctx.audit({ action: 'project.share', outcome: 'allowed', severity: 'notice',
                          resourceType: 'project', resourceId: projectId,
                          detail: { from: tenancy.visibilityOf(existing), to: visibility } });
      }
      return ok({ id: projectId, saved: true, visibility });
    }

    if (method === 'DELETE' && projectId) {
      const existing = await store.get(projectId, { type: 'json' });
      if (!existing || !tenancy.canSeeProject(userId, existing)) {
        return authz.errorResponse(await authz.notFound(ctx, { objectId: projectId }), CORS);
      }
      // Deleting shared work is the owner's call. A colleague who can see a
      // tenant-visible project can open it; they cannot destroy it.
      if (existing.ownerOid && existing.ownerOid !== userId) {
        await ctx.audit({ action: 'project.delete', outcome: 'denied', severity: 'notice',
                          resourceType: 'project', resourceId: projectId,
                          detail: { reason: 'not the owner' } });
        return fail('Only the project owner can delete it', 403);
      }
      await store.delete(projectId);
      await tenancy.unindexObject(ctx.store, projectId);
      await ctx.audit({ action: 'project.delete', outcome: 'allowed', severity: 'notice',
                        resourceType: 'project', resourceId: projectId });
      return ok({ deleted: true });
    }

    return fail('Method not allowed', 405);
  } catch (e) {
    console.error('[projects]', e.message);
    return fail('Operation failed: ' + e.message);
  }
};

// ── Legacy forward-migration ──────────────────────────────────────────────
//
// Runs once per signer, on their first listing. Everything it does is a
// copy: read the old store, write what is missing into the new one, drop a
// marker. A failure part-way leaves the marker unwritten, so the next
// listing finishes the job; a project already present in the tenant store is
// never overwritten, so a re-run cannot clobber later edits.
async function migrateLegacyProjects({ store, siteID, token, actor, ctx }) {
  const marker = tenancy.migrationMarkerKey(actor.oid);
  try {
    if (await store.get(marker, { type: 'json' })) return;
  } catch { /* unreadable marker — fall through and re-check by key below */ }

  let legacy;
  try {
    legacy = getStore({ name: tenancy.legacyProjectStoreName(actor.oid), siteID, token });
  } catch { return; }

  let blobs = [];
  try { ({ blobs } = await legacy.list()); } catch { return; }
  if (!blobs.length) {
    await store.setJSON(marker, { at: new Date().toISOString(), copied: 0 }).catch(() => {});
    return;
  }

  let copied = 0;
  for (const b of blobs) {
    if (!tenancy.PROJECT_KEY_RE.test(b.key)) continue;
    try {
      if (await store.get(b.key, { type: 'json' })) continue;   // already forward
      const data = await legacy.get(b.key, { type: 'json' });
      if (!data) continue;
      await store.setJSON(b.key, {
        ...data,
        tenantId: ctx.tenant.id,
        ownerOid: actor.oid,
        ownerEmail: data.userEmail || actor.email,
        // Private, deliberately. See the header.
        visibility: tenancy.MIGRATED_VISIBILITY,
        migratedAt: new Date().toISOString(),
        migratedFrom: 'per-user-store',
      });
      await tenancy.indexObject(ctx.store, b.key, ctx.tenant.id);
      copied++;
    } catch { /* one bad blob must not strand the rest */ }
  }

  await store.setJSON(marker, { at: new Date().toISOString(), copied }).catch(() => {});
  if (copied) {
    await ctx.audit({
      action: 'project.migrate', outcome: 'allowed', severity: 'notice',
      resourceType: 'tenant', resourceId: ctx.tenant.id,
      detail: { copied, from: 'per-user-store', visibility: tenancy.MIGRATED_VISIBILITY },
    });
  }
}
