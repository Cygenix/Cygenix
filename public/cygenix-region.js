/**
 * cygenix-region.js — currency/region selector
 *
 * Loaded by /pricing.html and /pick-plan.html (any page that shows prices
 * or needs to know the user's chosen currency). Provides:
 *
 *   window.CygenixRegion.get()                  → { currency, flag, symbol, code, label }
 *   window.CygenixRegion.set(currency)          → persists choice and dispatches change event
 *   window.CygenixRegion.onChange(handler)      → register a handler called on switch
 *   window.CygenixRegion.detect()               → returns the auto-detected currency (no persistence)
 *   window.CygenixRegion.SUPPORTED              → constant array of supported regions
 *
 * Currency choice persists in localStorage under 'cygenix_currency'. If
 * the user has never picked, we auto-detect from browser locale on first
 * call. Detection rules (in order):
 *   - en-US / en-CA → USD (en-CA gets USD as a sensible default; we don't
 *                          support CAD)
 *   - en-GB         → GBP
 *   - de / fr / it / es / nl / pt-PT / pl / sv / da / fi / cs / ... → EUR
 *   - anything else → GBP (it's the originating market and the safest fallback)
 *
 * IMPORTANT: this file does NOT update prices on the page. Each page is
 * responsible for listening to onChange and updating its own DOM. That
 * keeps the module reusable and means a page with no prices on it (like
 * a future "settings" page) can still use the dropdown without tripping
 * over price-element selectors.
 */
(function () {
  'use strict';

  const SUPPORTED = [
    { currency: 'GBP', symbol: '£', code: 'GBP', flag: '🇬🇧', label: 'GBP — £', regionLabel: 'United Kingdom' },
    { currency: 'USD', symbol: '$', code: 'USD', flag: '🇺🇸', label: 'USD — $', regionLabel: 'United States' },
    { currency: 'EUR', symbol: '€', code: 'EUR', flag: '🇪🇺', label: 'EUR — €', regionLabel: 'Eurozone' }
  ];
  const STORAGE_KEY = 'cygenix_currency';
  const DEFAULT     = 'GBP';

  function find(currency) {
    return SUPPORTED.find(r => r.currency === currency) || SUPPORTED[0];
  }

  function detect() {
    try {
      const locale = (navigator.languages && navigator.languages[0]) ||
                     navigator.language ||
                     'en-GB';
      const lc = String(locale || '').toLowerCase();

      // Region matching — most specific first
      if (lc.startsWith('en-us') || lc.startsWith('en-ca')) return 'USD';
      if (lc.startsWith('en-gb')) return 'GBP';

      // EUR-using locales. Not exhaustive but covers the common cases.
      const eurPrefixes = ['de', 'fr', 'it', 'es', 'nl', 'pt-pt', 'pl', 'sv', 'da', 'fi', 'cs',
                           'sk', 'sl', 'el', 'et', 'lv', 'lt', 'mt', 'ga', 'hu', 'hr'];
      if (eurPrefixes.some(p => lc.startsWith(p))) return 'EUR';

      // English fallbacks: en-AU, en-NZ, en-IE, en-IN, etc.
      // Default these to GBP (safest — closest to our "home" currency).
      return DEFAULT;
    } catch {
      return DEFAULT;
    }
  }

  function getCurrent() {
    let stored = null;
    try { stored = localStorage.getItem(STORAGE_KEY); } catch {}
    if (stored && SUPPORTED.some(r => r.currency === stored)) {
      return find(stored);
    }
    return find(detect());
  }

  function setCurrent(currency) {
    const region = find(currency);
    try { localStorage.setItem(STORAGE_KEY, region.currency); } catch {}
    // Broadcast change. Listeners (pages) update their own price displays.
    window.dispatchEvent(new CustomEvent('cygenix:region-change', { detail: region }));
    return region;
  }

  function onChange(handler) {
    const wrapped = (e) => handler(e.detail);
    window.addEventListener('cygenix:region-change', wrapped);
    // Return unsubscribe so callers can clean up if they ever need to.
    return () => window.removeEventListener('cygenix:region-change', wrapped);
  }

  window.CygenixRegion = {
    SUPPORTED,
    get:      getCurrent,
    set:      setCurrent,
    detect,
    onChange,
    find
  };
})();
