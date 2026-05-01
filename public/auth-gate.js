/**
 * auth-gate.js — Cygenix v3
 * Protects product pages using Azure Entra External ID (MSAL.js).
 * Reads MSAL's localStorage cache directly so it works before
 * msal-browser.min.js has loaded/initialized on the page.
 * Include in <head> of every protected page BEFORE other scripts.
 */
(function() {
  const PUBLIC = ['/', '/index.html', '/login.html', '/demo.html',
                  '/about.html', '/help.html', '/terms.html', '/privacy.html'];
  const path = window.location.pathname;
  if (PUBLIC.some(p => path === p || path.endsWith(p))) return;

  // Don't redirect-loop a user who just signed out
  if (sessionStorage.getItem('cygenix_just_signed_out') === '1') {
    sessionStorage.setItem('cygenix_redirect_after_login', window.location.href);
    window.location.replace('/login.html?reason=protected');
    return;
  }

  function hasMsalSession() {
    try {
      const nowSec = Math.floor(Date.now() / 1000);
      let hasAccount = false;
      let latestExpiry = 0;

      for (let i = 0; i < localStorage.length; i++) {
        const k = localStorage.key(i);
        if (!k) continue;
        // MSAL keys all contain one of these authority fragments
        if (!k.includes('-login.windows.net-') && !k.includes('.ciamlogin.com-')) continue;
        const raw = localStorage.getItem(k);
        if (!raw || raw[0] !== '{') continue;
        let obj;
        try { obj = JSON.parse(raw); } catch { continue; }
        if (!obj) continue;

        // Account record
        if (obj.username && obj.authorityType && obj.homeAccountId) {
          hasAccount = true;
        }
        // Token record with expiry — track the latest
        if (obj.credentialType && obj.expiresOn) {
          const exp = parseInt(obj.expiresOn, 10);
          if (!isNaN(exp) && exp > latestExpiry) latestExpiry = exp;
        }
      }

      if (!hasAccount) return false;
      // If we found tokens, require at least one to still be valid.
      // If we found an account but no token-with-expiry (rare), trust the
      // account and let MSAL refresh silently on the page.
      if (latestExpiry > 0 && latestExpiry <= nowSec) return false;
      return true;
    } catch {
      return false;
    }
  }

  if (!hasMsalSession()) {
    sessionStorage.setItem('cygenix_redirect_after_login', window.location.href);
    window.location.replace('/login.html?reason=protected');
  }
})();
