// ── Cygenix Connection Manager ────────────────────────────────────────────────
// Single source of truth for source/target DB connections.
// Stored in localStorage so they persist across sessions and pages.

const CONN_KEY = 'cygenix_project_connections';

const CygenixConnections = {
  // Load saved connections into sessionStorage (call on every page load)
  load() {
    try {
      const saved = JSON.parse(localStorage.getItem(CONN_KEY) || '{}');
      const keys = [
        'cygenix_src_conn_string', 'cygenix_src_conn_mode',
        'cygenix_conn_string', 'cygenix_fn_url', 'cygenix_fn_key', 'cygenix_conn_mode'
      ];
      keys.forEach(k => {
        if (saved[k]) sessionStorage.setItem(k, saved[k]);
      });
    } catch {}
  },

  // Save current sessionStorage connections to localStorage
  save() {
    try {
      const keys = [
        'cygenix_src_conn_string', 'cygenix_src_conn_mode',
        'cygenix_conn_string', 'cygenix_fn_url', 'cygenix_fn_key', 'cygenix_conn_mode'
      ];
      const data = {};
      keys.forEach(k => {
        const v = sessionStorage.getItem(k);
        if (v) data[k] = v;
      });
      localStorage.setItem(CONN_KEY, JSON.stringify(data));
    } catch {}
  },

  // Get all connection values
  get() {
    this.load();
    return {
      srcConnString: sessionStorage.getItem('cygenix_src_conn_string') || '',
      srcConnMode:   sessionStorage.getItem('cygenix_src_conn_mode')   || 'direct',
      tgtConnString: sessionStorage.getItem('cygenix_conn_string')     || '',
      tgtFnUrl:      sessionStorage.getItem('cygenix_fn_url')          || '',
      tgtFnKey:      sessionStorage.getItem('cygenix_fn_key')          || '',
      tgtConnMode:   sessionStorage.getItem('cygenix_conn_mode')       || 'azure',
    };
  },

  // Get the effective target connection string (fn url with key, or direct)
  tgtConn() {
    const c = this.get();
    if (c.tgtFnUrl) return c.tgtFnUrl + (c.tgtFnKey ? '?code=' + encodeURIComponent(c.tgtFnKey) : '');
    return c.tgtConnString;
  },

  srcConn() {
    return this.get().srcConnString;
  },

  // Clear all connections
  clear() {
    localStorage.removeItem(CONN_KEY);
    ['cygenix_src_conn_string','cygenix_src_conn_mode','cygenix_conn_string',
     'cygenix_fn_url','cygenix_fn_key','cygenix_conn_mode'].forEach(k => sessionStorage.removeItem(k));
  }
};

// Auto-load on every page
CygenixConnections.load();
