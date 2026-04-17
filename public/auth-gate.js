/**
 * auth-gate.js — Cygenix v2
 * Protects product pages using Azure Entra External ID (MSAL.js).
 * Include in <head> of every protected page BEFORE other scripts.
 */
(function() {
  const PUBLIC = ['/', '/index.html', '/login.html', '/demo.html',
                  '/about.html', '/help.html', '/terms.html', '/privacy.html'];
  const path = window.location.pathname;
  if (PUBLIC.some(p => path === p || path.endsWith(p))) return;

  // Check for valid Entra session in sessionStorage
  function isAuthenticated() {
    try {
      const acct = sessionStorage.getItem('cygenix_entra_account');
      if (!acct) return false;
      const { exp } = JSON.parse(acct);
      if (exp && Date.now() > exp) {
        sessionStorage.removeItem('cygenix_entra_account');
        return false;
      }
      return true;
    } catch { return false; }
  }

  if (!isAuthenticated()) {
    sessionStorage.setItem('cygenix_redirect_after_login', window.location.href);
    window.location.replace('/login.html?reason=protected');
  }
})();
