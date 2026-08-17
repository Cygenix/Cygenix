// cygenix-rbac.js — browser mirror of the server's role model.
//
// The server decides (netlify/functions/lib/rbac.js gates every call);
// this module only asks "who am I and what roles do I hold" so pages can
// hide controls the actor cannot use and label the ones they can. Hiding
// is a courtesy, never a control — a crafted API call still meets can()
// on the server (spec §3, principle 2).
//
// Usage:
//   const me = await CygenixRBAC.me();      // { roles, roleNames, collapsed, ... }
//   CygenixRBAC.has('PA')                   // after me() resolves
//   CygenixRBAC.isAdmin()                   // OW or PA
//   CygenixRBAC.applyGates(rootEl?)        // hides [data-rbac-role] elements
//                                           // whose roles the actor lacks
//
// Cached in sessionStorage for five minutes so page loads stay fast; a
// role change lands on the next cache expiry or hard reload. Revocation
// is enforced server-side immediately regardless (R-04).

(function () {
  'use strict';

  const ENDPOINT = '/.netlify/functions/rbac-admin?what=me';
  const CACHE_KEY = 'cygenix_rbac_me';
  const CACHE_MS = 5 * 60 * 1000;

  let _me = null;
  let _pending = null;

  function cached() {
    try {
      const raw = sessionStorage.getItem(CACHE_KEY);
      if (!raw) return null;
      const { at, me } = JSON.parse(raw);
      if (Date.now() - at > CACHE_MS) return null;
      return me;
    } catch { return null; }
  }

  async function fetchMe() {
    const token = (typeof getCygenixIdToken === 'function') ? getCygenixIdToken() : '';
    if (!token) return null;
    const r = await fetch(ENDPOINT, { headers: { Authorization: 'Bearer ' + token } });
    if (!r.ok) return null;
    const me = await r.json();
    try { sessionStorage.setItem(CACHE_KEY, JSON.stringify({ at: Date.now(), me })); } catch {}
    return me;
  }

  async function me(force) {
    if (_me && !force) return _me;
    if (!force) {
      const c = cached();
      if (c) { _me = c; return c; }
    }
    if (!_pending) {
      _pending = fetchMe().catch(() => null).then(m => { _pending = null; _me = m; return m; });
    }
    return _pending;
  }

  function roles() { return (_me && _me.roles) || []; }
  function has(role) { return roles().includes(role); }
  function isAdmin() { return has('OW') || has('PA'); }

  // Hide elements the actor's roles do not reach. data-rbac-role lists
  // acceptable roles comma-separated: <button data-rbac-role="PA,OW">.
  // No roles resolved (offline, signed out) leaves the page untouched —
  // the server still refuses anything that matters.
  async function applyGates(root) {
    const m = await me();
    if (!m || !m.roles) return;
    (root || document).querySelectorAll('[data-rbac-role]').forEach(el => {
      const want = el.dataset.rbacRole.split(',').map(s => s.trim()).filter(Boolean);
      if (!want.some(r => m.roles.includes(r))) el.style.display = 'none';
    });
  }

  function invalidate() {
    _me = null;
    try { sessionStorage.removeItem(CACHE_KEY); } catch {}
  }

  window.CygenixRBAC = { me, roles, has, isAdmin, applyGates, invalidate };
})();
