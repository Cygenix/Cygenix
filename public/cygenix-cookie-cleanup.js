/* cygenix-cookie-cleanup.js — defensive cookie-banner hider
 *
 * Loaded by every page that includes /cookie-consent.js. If the user has
 * dismissed cookies (flag set in localStorage or sessionStorage), this
 * hides any lingering cookie banner that the cookie-consent library left
 * behind.
 *
 * NOTE on the "cookie settings" button (#cc-settings-btn):
 *   This is how users revoke/change consent — required for GDPR/ePrivacy
 *   compliance. We HIDE it off-screen rather than remove it, so the
 *   library's click handler stays attached. A "Cookie preferences" link
 *   at the bottom of the main nav programmatically clicks this hidden
 *   button, re-opening the consent banner.
 *
 * Runs three times:
 *   1. Immediately on script load
 *   2. On DOMContentLoaded (if still loading)
 *   3. Every 200ms for 2 seconds, in case the library injects late
 */
(function(){
  const DISMISS_KEYS = [
    'cookieConsent','cookie_consent','cookies_accepted','cookie-accepted',
    'cookiesAccepted','cookieDismissed','cookie_banner_dismissed',
    'cygenix_cookie_consent','cygenix_cookies','cookieConsentDismissed'
  ];
  // Banner / notice containers — remove entirely when dismissed
  const REMOVE_SELECTORS = [
    '#cookie-banner','#cookieBanner','#cookie-consent','#cookieConsent',
    '.cookie-banner','.cookieBanner','.cookie-consent','.cookieConsent',
    '.cookie-notice','#cookie-notice','.cookies-banner'
  ];
  // Settings/preferences buttons — HIDE (keep in DOM), so a nav link can
  // still trigger them programmatically
  const HIDE_SELECTORS = [
    '#cc-settings-btn','#cookie-icon','.cookie-icon',
    '.cookie-settings-btn','.cookie-fab'
  ];

  function anyDismissFlag(){
    for (const k of DISMISS_KEYS){
      try {
        if (localStorage.getItem(k) || sessionStorage.getItem(k)) return true;
      } catch {}
    }
    return false;
  }

  function cleanup(){
    if (!anyDismissFlag()) return;
    REMOVE_SELECTORS.forEach(sel => {
      document.querySelectorAll(sel).forEach(el => el.remove());
    });
    HIDE_SELECTORS.forEach(sel => {
      document.querySelectorAll(sel).forEach(el => {
        // Keep in DOM but visually & positionally gone. !important to beat
        // inline styles set by the cookie library.
        el.style.setProperty('position', 'fixed', 'important');
        el.style.setProperty('left', '-9999px', 'important');
        el.style.setProperty('top', '-9999px', 'important');
        el.style.setProperty('width', '0', 'important');
        el.style.setProperty('height', '0', 'important');
        el.style.setProperty('opacity', '0', 'important');
        el.style.setProperty('pointer-events', 'none', 'important');
        el.setAttribute('aria-hidden', 'true');
        el.setAttribute('tabindex', '-1');
      });
    });
  }

  // Public: trigger the cookie library's settings flow by clicking whichever
  // hidden settings button exists. Used by the "Cookie preferences" nav link.
  window.openCookiePreferences = function(){
    for (const sel of HIDE_SELECTORS){
      const el = document.querySelector(sel);
      if (el){ el.click(); return; }
    }
    console.warn('[cookie-cleanup] No cookie settings button found to open.');
  };

  cleanup();
  if (document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', cleanup);
  }
  let tries = 0;
  const interval = setInterval(() => {
    cleanup();
    if (++tries > 10) clearInterval(interval);
  }, 200);
})();
