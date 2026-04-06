// project-manager.js
// Drop this script into any page that needs auth + project management.
// Requires: <script src="https://identity.netlify.com/v1/netlify-identity-widget.js"></script>
// Include BEFORE your page scripts.

(function() {
  'use strict';

  // ── Auth state ───────────────────────────────────────────────────────────────
  const identity = window.netlifyIdentity;
  let _currentUser = null;

  // Redirect to login if not authenticated
  identity.on('init', user => {
    _currentUser = user;
    if (!user && !window.location.pathname.includes('login')) {
      window.location.href = '/login.html';
    }
    if (user) {
      updateUserUI(user);
      loadProjectsMenu();
    }
  });

  identity.on('login', user => {
    _currentUser = user;
    updateUserUI(user);
    loadProjectsMenu();
  });

  identity.on('logout', () => {
    _currentUser = null;
    window.location.href = '/login.html';
  });

  identity.init();

  // ── Public API ───────────────────────────────────────────────────────────────
  window.CygenixAuth = {
    getUser:  () => _currentUser,
    getToken: () => _currentUser?.token?.access_token || null,
    logout:   () => identity.logout(),

    // Save current project state
    saveProject: async function(projectData, projectName) {
      const token = this.getToken();
      if (!token) throw new Error('Not authenticated');

      const body = {
        project: {
          ...projectData,
          name: projectName,
          updatedAt: new Date().toISOString(),
          userEmail: _currentUser?.email,
          userName: _currentUser?.user_metadata?.full_name || _currentUser?.email,
        },
        name: projectName,
        id: projectData.id || null,
      };

      const res = await fetch('/api/projects' + (projectData.id ? '?id=' + projectData.id : ''), {
        method: projectData.id ? 'PUT' : 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Bearer ' + token,
        },
        body: JSON.stringify(body),
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || 'Failed to save project');
      }
      return res.json();
    },

    // Load a specific project
    loadProject: async function(projectId) {
      const token = this.getToken();
      if (!token) throw new Error('Not authenticated');
      const res = await fetch('/api/projects?id=' + projectId, {
        headers: { 'Authorization': 'Bearer ' + token }
      });
      if (!res.ok) throw new Error('Project not found');
      return res.json();
    },

    // List all projects
    listProjects: async function() {
      const token = this.getToken();
      if (!token) throw new Error('Not authenticated');
      const res = await fetch('/api/projects', {
        headers: { 'Authorization': 'Bearer ' + token }
      });
      if (!res.ok) throw new Error('Could not load projects');
      return res.json();
    },

    // Delete a project
    deleteProject: async function(projectId) {
      const token = this.getToken();
      if (!token) throw new Error('Not authenticated');
      const res = await fetch('/api/projects?id=' + projectId, {
        method: 'DELETE',
        headers: { 'Authorization': 'Bearer ' + token }
      });
      return res.json();
    },
  };

  // ── UI helpers ───────────────────────────────────────────────────────────────
  function updateUserUI(user) {
    const nameEl   = document.getElementById('user-name');
    const emailEl  = document.getElementById('user-email');
    const avatarEl = document.getElementById('user-avatar');
    const name = user?.user_metadata?.full_name || user?.email?.split('@')[0] || 'User';
    const initials = name.split(' ').map(n => n[0]).join('').slice(0, 2).toUpperCase();

    if (nameEl)   nameEl.textContent  = name;
    if (emailEl)  emailEl.textContent = user?.email || '';
    if (avatarEl) avatarEl.textContent = initials;
  }

  async function loadProjectsMenu() {
    const listEl = document.getElementById('projects-menu-list');
    if (!listEl) return;
    try {
      const { projects } = await window.CygenixAuth.listProjects();
      if (!projects || projects.length === 0) {
        listEl.innerHTML = '<div style="padding:0.75rem 1rem;font-size:12px;color:var(--text3)">No saved projects yet</div>';
        return;
      }
      listEl.innerHTML = projects
        .sort((a, b) => new Date(b.updatedAt) - new Date(a.updatedAt))
        .map(p => `
          <div class="project-menu-item" onclick="loadProjectById('${p.id}')">
            <div class="pmeta">
              <span class="pname">${p.name}</span>
              <span class="pstatus ${p.status === 'complete' ? 'green' : 'amber'}">${p.status === 'complete' ? 'Complete' : 'In progress'}</span>
            </div>
            <div class="pdetail">${p.sourceFile || '—'} → ${p.targetDb || '—'} · ${(p.totalRows||0).toLocaleString()} rows · ${formatDate(p.updatedAt)}</div>
          </div>`).join('');
    } catch(e) {
      listEl.innerHTML = '<div style="padding:0.75rem 1rem;font-size:12px;color:var(--red)">Could not load projects</div>';
    }
  }

  function formatDate(iso) {
    if (!iso) return '';
    const d = new Date(iso);
    return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
  }

  // Expose loadProjectsMenu so dashboard can call it after saving
  window._refreshProjectsMenu = loadProjectsMenu;

})();
