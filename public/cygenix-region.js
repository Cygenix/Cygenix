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
      // ── Step 1: timezone-based detection ──────────────────────────────
      // This is the most reliable signal we have. Browser language often
      // says 'en-US' for users physically in the UK, EU, AU, etc. because
      // their browser shipped with US English defaults and they never
      // changed it. Timezone tells us where the machine actually is.
      let tz = '';
      try { tz = Intl.DateTimeFormat().resolvedOptions().timeZone || ''; } catch {}

      if (tz === 'Europe/London') return 'GBP';
      if (tz.startsWith('Europe/')) {
        // Most Europe/* timezones are EUR (Berlin, Paris, Madrid, Rome,
        // Amsterdam, Dublin, etc.). A handful of non-EUR ones are
        // explicitly mapped here so we don't mis-classify them.
        const nonEurEurope = {
          'Europe/London':    'GBP',  // already handled above, defensive
          'Europe/Jersey':    'GBP',
          'Europe/Guernsey':  'GBP',
          'Europe/Isle_of_Man':'GBP',
          'Europe/Zurich':    'GBP',  // CHF really, but we don't support CHF — closest is GBP
          'Europe/Oslo':      'GBP',  // NOK; we don't support, fall back to GBP
          'Europe/Stockholm': 'GBP',  // SEK; ditto
          'Europe/Copenhagen':'GBP',  // DKK; ditto
          'Europe/Moscow':    'GBP',  // RUB; ditto
          'Europe/Istanbul':  'GBP',  // TRY; ditto
          'Europe/Kiev':      'GBP',
          'Europe/Warsaw':    'GBP'   // PLN; ditto. Polish customers can override.
        };
        return nonEurEurope[tz] || 'EUR';
      }

      if (tz.startsWith('America/')) {
        // North America defaults to USD. Real Canadian/Mexican customers
        // can override with the dropdown.
        return 'USD';
      }

      // ── Step 2: language fallback for unusual timezones ───────────────
      // If we got here, the timezone wasn't European or American. Use the
      // language list as a softer hint. Walk ALL entries (not just [0]),
      // because browsers sometimes report ['en','en-GB'] in that order.
      const langs = (navigator.languages && navigator.languages.length)
        ? navigator.languages
        : [navigator.language || 'en-GB'];

      for (const raw of langs) {
        const lc = String(raw || '').toLowerCase();
        if (lc.startsWith('en-us') || lc.startsWith('en-ca')) return 'USD';
        if (lc.startsWith('en-gb') || lc.startsWith('en-ie')) return 'GBP';
        const eurPrefixes = ['de', 'fr', 'it', 'es', 'nl', 'pt-pt', 'pl', 'sv',
                             'da', 'fi', 'cs', 'sk', 'sl', 'el', 'et', 'lv',
                             'lt', 'mt', 'ga', 'hu', 'hr'];
        if (eurPrefixes.some(p => lc.startsWith(p))) return 'EUR';
      }

      // ── Step 3: ultimate fallback ─────────────────────────────────────
      // Unknown locale and unknown timezone. Default to GBP since this is
      // a UK product and that's the safest assumption for ambiguous cases.
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
