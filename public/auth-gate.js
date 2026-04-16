/**
 * auth-gate.js
 * Protects all product pages — redirects unauthenticated users to login.
 * Include in <head> of every protected page BEFORE any other scripts.
 * 
 * Phase 1 (now):    Free registration — anyone can sign up
 * Phase 2 (later):  Subscription required — check plan status in Cosmos DB
 */

(function() {
  // Pages that are always public — no auth needed
  const PUBLIC_PAGES = ['/', '/index.html', '/login.html', '/demo.html', '/about.html', '/help.html', '/privacy-policy'];

  const path = window.location.pathname;
  const isPublic = PUBLIC_PAGES.some(p => path === p || path.endsWith(p));
  if (isPublic) return; // don't gate public pages

  // ── Check if user is authenticated ─────────────────────────────────────────
  function isAuthenticated() {
    // Check session token
    const token = sessionStorage.getItem('cygenix_token');
    const expires = sessionStorage.getItem('cygenix_expires');
    if (!token) return false;

    // Check token hasn't expired
    if (expires && Date.now() > parseInt(expires)) {
      // Try to refresh — if refresh token exists let the page handle it
      const refresh = localStorage.getItem('cygenix_refresh_token');
      if (!refresh) {
        sessionStorage.clear();
        return false;
      }
    }
    return true;
  }

  // ── Redirect to login if not authenticated ──────────────────────────────────
  if (!isAuthenticated()) {
    // Store the page they were trying to reach so we can redirect after login
    sessionStorage.setItem('cygenix_redirect_after_login', window.location.href);
    window.location.replace('/login.html?reason=protected');
  }

})();
