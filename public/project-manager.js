// project-manager.js
// Auth + project management using GoTrue API directly (no widget needed).
// Include this on every protected page BEFORE your page scripts.

(function () {
  'use strict';

  // ── Session helpers ──────────────────────────────────────────────────────────
  function getToken()   { return sessionStorage.getItem('cygenix_token') || null; }
  function getUser()    { try { return JSON.parse(sessionStorage.getItem('cygenix_user')); } catch { return null; } }
  function isExpired()  { return Date.now() > parseInt(sessionStorage.getItem('cygenix_expires') || '0'); }
  function clearSession() {
    ['cygenix_token','cygenix_user','cygenix_expires'].forEach(k => sessionStorage.removeItem(k));
  }

  // Redirect to login if not authenticated
  if (!getToken() || isExpired()) {
    clearSession();
    if (!window.location.pathname.includes('login')) {
      window.location.href = '/login.html';
    }
  }

  // ── Public API ────────────────────────────────────────────────────────────────
  window.CygenixAuth = {
    getUser,
    getToken,

    logout() {
      clearSession();
      window.location.href = '/login.html';
    },

    async saveProject(projectData, projectName) {
      const token = getToken();
      if (!token) throw new Error('Not authenticated');
      const body = {
        project: {
          ...projectData,
          name: projectName,
          updatedAt: new Date().toISOString(),
          userEmail: getUser()?.email,
          userName:  getUser()?.user_metadata?.full_name || getUser()?.email,
        },
        name: projectName,
        id: projectData.id || null,
      };
      const res = await fetch(
        '/.netlify/functions/projects' + (projectData.id ? '?id=' + projectData.id : ''),
        {
          method: projectData.id ? 'PUT' : 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
          body: JSON.stringify(body),
        }
      );
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || 'Failed to save project');
      }
      return res.json();
    },

    async loadProject(id) {
      const token = getToken();
      if (!token) throw new Error('Not authenticated');
      const res = await fetch('/.netlify/functions/projects?id=' + id, {
        headers: { 'Authorization': 'Bearer ' + token }
      });
      if (!res.ok) throw new Error('Project not found');
      return res.json();
    },

    async listProjects() {
      const token = getToken();
      if (!token) throw new Error('Not authenticated');
      const res = await fetch('/.netlify/functions/projects', {
        headers: { 'Authorization': 'Bearer ' + token }
      });
      if (!res.ok) throw new Error('Could not load projects');
      return res.json();
    },

    async deleteProject(id) {
      const token = getToken();
      if (!token) throw new Error('Not authenticated');
      const res = await fetch('/.netlify/functions/projects?id=' + id, {
        method: 'DELETE',
        headers: { 'Authorization': 'Bearer ' + token }
      });
      return res.json();
    },
  };

  // ── Update UI with user info once DOM is ready ────────────────────────────────
  function updateUserUI() {
    const user = getUser();
    if (!user) return;
    const name     = user.user_metadata?.full_name || user.email?.split('@')[0] || 'User';
    const initials = name.split(' ').map(n => n[0]).join('').slice(0, 2).toUpperCase();
    const nameEl   = document.getElementById('user-name');
    const emailEl  = document.getElementById('user-email');
    const avatarEl = document.getElementById('user-avatar');
    if (nameEl)   nameEl.textContent   = name;
    if (emailEl)  emailEl.textContent  = user.email || '';
    if (avatarEl) avatarEl.textContent = initials;
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', updateUserUI);
  } else {
    updateUserUI();
  }

})();
