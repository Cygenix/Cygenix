/* cygenix-cookie-cleanup.js — defensive cookie-banner hider
 *
 * Loaded by every page that includes /cookie-consent.js. If the user has
 * dismissed cookies (flag set in localStorage or sessionStorage), this
 * removes any lingering cookie banner / floating icon that the
 * cookie-consent library left behind.
 *
 * Runs three times:
 *   1. Immediately on script load
 *   2. On DOMContentLoaded (if still loading)
 *   3. Every 200ms for 2 seconds, in case the library injects late
 *
 * Without seeing cookie-consent.js source I'm guessing at selector names;
 * the common patterns are covered. If the banner still appears after this
 * runs, add its specific selector to the SELECTORS array below.
 */
(function(){
  const DISMISS_KEYS = [
    'cookieConsent','cookie_consent','cookies_accepted','cookie-accepted',
    'cookiesAccepted','cookieDismissed','cookie_banner_dismissed',
    'cygenix_cookie_consent','cygenix_cookies','cookieConsentDismissed'
  ];
  const SELECTORS = [
    // Banner containers
    '#cookie-banner','#cookieBanner','#cookie-consent','#cookieConsent',
    '.cookie-banner','.cookieBanner','.cookie-consent','.cookieConsent',
    '.cookie-notice','#cookie-notice','.cookies-banner',
    // Floating "cookie settings" icons that some libraries leave behind
    '#cookie-icon','.cookie-icon','.cookie-settings-btn','.cookie-fab'
  ];

  function anyDismissFlag(){
    for (const k of DISMISS_KEYS){
      try {
        if (localStorage.getItem(k) || sessionStorage.getItem(k)) return true;
      } catch {}
    }
    return false;
  }

  function hideBanner(){
    if (!anyDismissFlag()) return;
    SELECTORS.forEach(sel => {
      document.querySelectorAll(sel).forEach(el => el.remove());
    });
  }

  hideBanner();
  if (document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', hideBanner);
  }
  // Also run for the first couple of seconds in case the cookie lib injects late
  let tries = 0;
  const interval = setInterval(() => {
    hideBanner();
    if (++tries > 10) clearInterval(interval);
  }, 200);
})();
