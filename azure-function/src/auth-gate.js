/**
 * auth-gate.js — Cygenix v4
 * Protects product pages using Azure Entra External ID (MSAL.js).
 *
 * Two-layer gate:
 *   Layer 1 (sync): MSAL session check — reads localStorage cache directly so
 *                   it works before msal-browser.min.js has loaded. If no
 *                   session, redirect to /login.html immediately.
 *   Layer 2 (async): tier check — for signed-in users, fetch /api/data/whoami
 *                    and decide whether they should be on this page or
 *                    redirected to /pick-plan.html.
 *
 * Layer 2 only runs once the page DOM is parsing, so the visible flash is
 * minimal but non-zero. We hide the body until layer 2 resolves; if layer 2
 * passes, body is unhidden, otherwise we redirect before showing anything.
 *
 * Include in <head> of every protected page BEFORE other scripts.
 */
(function() {
  // ── Path classification ──────────────────────────────────────────────────
  // PUBLIC: no auth required at all.
  // TIER_EXEMPT: signed-in users only, but not redirected to pick-plan even
  //              if they have no tier. Pick-plan itself, welcome (post-checkout
  //              landing), and login pages must be in here or we'd loop.
  const PUBLIC = ['/', '/index.html', '/login.html', '/demo.html',
                  '/about.html', '/help.html', '/terms.html', '/privacy.html',
                  '/pricing', '/pricing.html', '/register.html'];
  const TIER_EXEMPT = ['/pick-plan.html', '/welcome.html'];

  const path = window.location.pathname;
  const matches = list => list.some(p => path === p || path.endsWith(p));

  if (matches(PUBLIC)) return;

  // Don't redirect-loop a user who just signed out
  if (sessionStorage.getItem('cygenix_just_signed_out') === '1') {
    sessionStorage.setItem('cygenix_redirect_after_login', window.location.href);
    window.location.replace('/login.html?reason=protected');
    return;
  }

  // ── Layer 1: synchronous MSAL session check ─────────────────────────────
  function hasMsalSession() {
    try {
      const nowSec = Math.floor(Date.now() / 1000);
      let hasAccount = false;
      let latestExpiry = 0;

      for (let i = 0; i < localStorage.length; i++) {
        const k = localStorage.key(i);
        if (!k) continue;
        if (!k.includes('-login.windows.net-') && !k.includes('.ciamlogin.com-')) continue;
        const raw = localStorage.getItem(k);
        if (!raw || raw[0] !== '{') continue;
        let obj;
        try { obj = JSON.parse(raw); } catch { continue; }
        if (!obj) continue;

        if (obj.username && obj.authorityType && obj.homeAccountId) {
          hasAccount = true;
        }
        if (obj.credentialType && obj.expiresOn) {
          const exp = parseInt(obj.expiresOn, 10);
          if (!isNaN(exp) && exp > latestExpiry) latestExpiry = exp;
        }
      }

      if (!hasAccount) return false;
      if (latestExpiry > 0 && latestExpiry <= nowSec) return false;
      return true;
    } catch {
      return false;
    }
  }

  if (!hasMsalSession()) {
    sessionStorage.setItem('cygenix_redirect_after_login', window.location.href);
    window.location.replace('/login.html?reason=protected');
    return;
  }

  // ── Layer 2: tier check (async) ──────────────────────────────────────────
  // We're signed in. Now check whether we have a tier. Tier-exempt pages
  // don't need this — they're allowed for any signed-in user.
  if (matches(TIER_EXEMPT)) return;

  // Hide body until tier check resolves to avoid a flash of the protected
  // page for users who are about to be redirected. We use a style block
  // injected into <head>; if the gate fails or takes too long, the timeout
  // below restores visibility so a network glitch never leaves the user
  // staring at a blank page forever.
  const styleEl = document.createElement('style');
  styleEl.id = 'cygenix-tier-gate-style';
  styleEl.textContent = 'body{visibility:hidden !important}';
  // Only inject if <head> exists. Script in <head> runs synchronously, so
  // document.head should be available; defensive fallback for edge cases.
  (document.head || document.documentElement).appendChild(styleEl);

  // Safety net: if the whoami call hangs or fails for >8s, unhide the body
  // and let the page render. Better to show the page (and let the user see
  // an error in console) than to leave them with a blank screen.
  const failsafeTimer = setTimeout(() => {
    const s = document.getElementById('cygenix-tier-gate-style');
    if (s) s.remove();
    console.warn('[auth-gate] whoami timed out — failing open. Tier check skipped for this page load.');
  }, 8000);

  // Helper: get userId from MSAL cache (mirrors logic in cygenix-cosmos-sync.js
  // but inlined so auth-gate has no external deps). Returns lowercased email
  // or null.
  function readUserIdFromMsal() {
    try {
      for (let i = 0; i < localStorage.length; i++) {
        const k = localStorage.key(i);
        if (!k) continue;
        if (!k.includes('-login.windows.net-') && !k.includes('.ciamlogin.com-')) continue;
        const raw = localStorage.getItem(k);
        if (!raw || raw[0] !== '{') continue;
        let obj;
        try { obj = JSON.parse(raw); } catch { continue; }
        if (obj && obj.username && obj.authorityType && obj.homeAccountId) {
          const id = String(obj.username || '').trim().toLowerCase();
          if (id) return id;
        }
      }
    } catch {}
    return null;
  }

  const userId = readUserIdFromMsal();
  if (!userId) {
    // Layer 1 said we have a session but we can't extract the email.
    // Fail safe — let the page render rather than redirect-loop.
    clearTimeout(failsafeTimer);
    const s = document.getElementById('cygenix-tier-gate-style');
    if (s) s.remove();
    return;
  }

  const API_BASE  = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/data';
  const FUNC_CODE = 'WjSmoWxgtNdGnO_I5nKIspRUQqKCR1knsXgVmJr3dyYuAzFu-or-5Q==';

  fetch(`${API_BASE}/whoami?code=${FUNC_CODE}`, {
    method: 'GET',
    headers: { 'Content-Type': 'application/json', 'x-user-id': userId }
  })
    .then(r => r.ok ? r.json() : null)
    .then(data => {
      clearTimeout(failsafeTimer);
      const s = document.getElementById('cygenix-tier-gate-style');

      // No data — fail open. Network or server issue, don't lock user out.
      if (!data) {
        if (s) s.remove();
        console.warn('[auth-gate] whoami returned no data — failing open.');
        return;
      }

      // Admin and demo users bypass tier checks entirely.
      if (data.role === 'admin' || data.role === 'demo') {
        if (s) s.remove();
        return;
      }

      // Has a tier and it's in good standing — allow.
      const goodStatuses = ['trialing', 'active', 'past_due'];
      if (data.tier && goodStatuses.includes(data.tier_status)) {
        if (s) s.remove();
        return;
      }

      // No tier, or tier in a bad state — send to pick-plan.
      // Stash where they were trying to go so pick-plan could in theory
      // bounce them back after checkout. Currently welcome.html handles
      // post-checkout routing, so this is forward-compat.
      sessionStorage.setItem('cygenix_redirect_after_plan', window.location.href);
      window.location.replace('/pick-plan.html?reason=' +
        encodeURIComponent(data.tier_status || 'no-tier'));
    })
    .catch(err => {
      clearTimeout(failsafeTimer);
      const s = document.getElementById('cygenix-tier-gate-style');
      if (s) s.remove();
      console.warn('[auth-gate] whoami failed — failing open:', err);
    });
})();
