// project-manager.js
// Auth + project management using GoTrue API directly.

(function () {
  'use strict';

  const FN = '/.netlify/functions/projects';

  // Check localStorage as well as sessionStorage: sessionStorage is per-tab,
  // so the old sessionStorage-only reads bounced validly signed-in users to
  // login whenever they opened this page in a NEW tab (and cleared their
  // legacy session keys on the way out).
  function getToken()   { return sessionStorage.getItem('cygenix_token') || localStorage.getItem('cygenix_token') || null; }
  function getUser()    {
    try { return JSON.parse(sessionStorage.getItem('cygenix_user') || localStorage.getItem('cygenix_user')); }
    catch { return null; }
  }
  function isExpired()  {
    const exp = parseInt(sessionStorage.getItem('cygenix_expires') || localStorage.getItem('cygenix_expires') || '0');
    if (exp) return Date.now() > exp;
    // No legacy expiry stamp — fall back to the Entra account record.
    try {
      const acc = JSON.parse(localStorage.getItem('cygenix_entra_account') || 'null');
      if (acc && acc.exp) return Date.now() > acc.exp;
    } catch {}
    return true;
  }
  function clearSession() {
    ['cygenix_token','cygenix_user','cygenix_expires'].forEach(k => {
      sessionStorage.removeItem(k);
      localStorage.removeItem(k);
    });
  }

  // Redirect to login if not authenticated
  if (!getToken() || isExpired()) {
    clearSession();
    if (!window.location.pathname.includes('login')) {
      window.location.href = '/login';
    }
  }

  async function apiFetch(path, options = {}) {
    const token = getToken();
    const res = await fetch(FN + path, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + token,
        ...(options.headers || {}),
      },
    });
    const data = await res.json().catch(() => ({ error: 'Invalid response' }));
    if (!res.ok) throw new Error(data.error || 'Request failed (' + res.status + ')');
    return data;
  }

  window.CygenixAuth = {
    getUser,
    getToken,

    logout() {
      clearSession();
      window.location.href = '/login';
    },

    async saveProject(projectData, projectName) {
      const method = projectData.id ? 'PUT' : 'POST';
      const path   = projectData.id ? '?id=' + projectData.id : '';
      return apiFetch(path, {
        method,
        body: JSON.stringify({
          project: {
            ...projectData,
            name: projectName,
            updatedAt: new Date().toISOString(),
            userEmail: getUser()?.email,
            userName:  getUser()?.user_metadata?.full_name || getUser()?.email,
          },
          name: projectName,
        }),
      });
    },

    async loadProject(id) {
      return apiFetch('?id=' + id);
    },

    async listProjects() {
      return apiFetch('');
    },

    async deleteProject(id) {
      return apiFetch('?id=' + id, { method: 'DELETE' });
    },
  };

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
