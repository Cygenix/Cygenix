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

  function isAuthenticated() {
    try {
      // Check sessionStorage first, then localStorage (MSAL uses localStorage)
      const raw = sessionStorage.getItem('cygenix_entra_account')
               || localStorage.getItem('cygenix_entra_account');
      if (!raw) return false;
      const { exp } = JSON.parse(raw);
      if (exp && Date.now() > exp) {
        sessionStorage.removeItem('cygenix_entra_account');
        localStorage.removeItem('cygenix_entra_account');
        return false;
      }
      // Copy to sessionStorage if only in localStorage
      if (!sessionStorage.getItem('cygenix_entra_account')) {
        sessionStorage.setItem('cygenix_entra_account', raw);
      }
      return true;
    } catch { return false; }
  }

  if (!isAuthenticated()) {
    sessionStorage.setItem('cygenix_redirect_after_login', window.location.href);
    window.location.replace('/login.html?reason=protected');
  }
})();
