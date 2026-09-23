/* ============================================================================
   cygenix-ga4.js — Google Analytics 4, on the public site only, and only
   after somebody has said yes.
   ----------------------------------------------------------------------------
   WHAT THIS IS FOR
   Anonymous visitor statistics for the marketing pages: how many people
   reach the home page, whether they get as far as pricing, whether they
   register. Nothing about a signed-in customer's work. The seven public
   pages load this file; no page behind the sign-in does, and there is a
   test that fails the build if one starts to.

   WHY IT DOES NOT LOAD GOOGLE STRAIGHT AWAY
   Google's own Consent Mode pattern is to load gtag.js immediately with
   every storage signal denied, so it sits there sending cookieless pings
   until consent arrives. That is defensible under UK PECR — nothing is
   written to the device while storage is denied — but it is not quiet: a
   denied ping still carries the visitor's IP address to Google, and it
   happens before anyone has agreed to anything. The stricter reading is
   that no request should go to an analytics vendor at all until the
   visitor has chosen, and that is what this file does.

   So the order is:

     1. This file runs. It creates the dataLayer and the gtag() shim, and
        sets every consent signal to DENIED. This happens synchronously,
        before anything could possibly load Google, which is the whole
        point of putting the tag immediately after <head>.
     2. Nothing else happens. No network request. No cookie.
     3. If the visitor has already accepted on a previous visit, or when
        they press Accept on the banner, the consent signal is updated to
        granted and only THEN is Google's script injected.

   Consent is owned by cookie-consent.js, not by this file. This file never
   shows UI, never writes the consent record and never decides policy — it
   reads the record, listens for 'cygenix:cookie-consent', and acts. One
   owner for the decision, one owner for the tag.

   THE TWO FLAGS, AND WHY NEITHER IS RESET BY A CALLBACK
   _injected guards the <script> injection and _denied guards the revoke
   path. Both are set BEFORE the work they guard, never inside the load
   handler, because a flag that a callback resets is a flag that lets the
   same work start twice when the callback is slow — the render-loop
   hazard this codebase has been bitten by before. A failed script load
   leaves _injected true on purpose: retrying a blocked request on every
   consent event would be a loop, and an ad-blocker is not a transient
   fault.

   REVOKING
   A script already fetched cannot be unfetched, so revoking does two
   things it can do: tells Google to stop using storage, and deletes the
   two cookies GA4 has set. The next page load then starts clean, because
   nothing is injected without a granted record.
   ========================================================================== */
(function (root) {
  'use strict';

  var MEASUREMENT_ID = 'G-K3NJP5GX2G';
  var CONSENT_KEY = 'cygenix_cookie_consent';
  var SRC = 'https://www.googletagmanager.com/gtag/js?id=' + MEASUREMENT_ID;

  var _injected = false;
  var _denied = false;

  // ── The gtag shim ───────────────────────────────────────────────────────
  // Defined before Google's script exists so calls made now are queued on
  // the dataLayer and replayed when it arrives. `arguments` rather than a
  // rest parameter: gtag reads the arguments object itself.
  root.dataLayer = root.dataLayer || [];
  function gtag() { root.dataLayer.push(arguments); }
  root.gtag = root.gtag || gtag;

  // ── Default: everything denied ──────────────────────────────────────────
  // The four v2 signals. ad_storage, ad_user_data and ad_personalization
  // are denied here and are never granted anywhere in this file: Cygenix
  // does not advertise, so there is no path that turns them on. Accept
  // grants analytics_storage and nothing else.
  gtag('consent', 'default', {
    analytics_storage: 'denied',
    ad_storage: 'denied',
    ad_user_data: 'denied',
    ad_personalization: 'denied',
    wait_for_update: 500,
  });

  // ── Reading the consent record ──────────────────────────────────────────
  // Storage throws in a private window and in some embedded browsers, and
  // a thrown read must mean "no consent", never an uncaught error on a
  // marketing page. Any parse failure is treated the same way.
  function analyticsGranted() {
    try {
      var raw = root.localStorage.getItem(CONSENT_KEY);
      if (!raw) return false;
      var rec = JSON.parse(raw);
      return !!(rec && rec.analytics === true);
    } catch (e) { return false; }
  }

  // ── Granting ────────────────────────────────────────────────────────────
  function grant() {
    if (_denied) _denied = false;
    gtag('consent', 'update', { analytics_storage: 'granted' });
    inject();
  }

  function inject() {
    if (_injected) return;
    _injected = true;                 // set BEFORE the work it guards
    try {
      var s = root.document.createElement('script');
      s.async = true;
      s.src = SRC;
      root.document.head.appendChild(s);
      gtag('js', new Date());
      // No anonymize_ip: GA4 truncates the address on collection and the
      // parameter is ignored, so passing it would only suggest a control
      // that is not doing anything.
      gtag('config', MEASUREMENT_ID);
    } catch (e) {
      // A page with no head, or a blocked DOM. Nothing to recover.
    }
  }

  // ── Revoking ────────────────────────────────────────────────────────────
  // Deletes GA4's two cookies on every host suffix the page could have set
  // them on, because a cookie written for ".cygenix.co.uk" is not removed
  // by a delete scoped to "www.cygenix.co.uk".
  function revoke() {
    if (_denied) return;
    _denied = true;                   // set BEFORE the work it guards
    gtag('consent', 'update', { analytics_storage: 'denied' });
    try {
      var names = ['_ga', '_ga_' + MEASUREMENT_ID.replace(/^G-/, ''), '_gid'];
      var host = root.location.hostname;
      var domains = ['', host];
      // Every parent of the host, down to but not including the last
      // label. Walking up beats guessing the registrable domain: the
      // last two labels are the right answer for cygenix.com and the
      // wrong one for cygenix.co.uk, and a delete aimed at a domain the
      // cookie was not set on is simply ignored.
      var parts = host.split('.');
      for (var i = 0; i < parts.length - 1; i++) domains.push('.' + parts.slice(i).join('.'));
      names.forEach(function (n) {
        domains.forEach(function (d) {
          root.document.cookie = n + '=; expires=Thu, 01 Jan 1970 00:00:01 GMT; path=/'
            + (d ? '; domain=' + d : '');
        });
      });
    } catch (e) { /* cookies unavailable; the denied signal still stands */ }
  }

  function apply(granted) { if (granted) grant(); else revoke(); }

  // ── Now, and on every change ────────────────────────────────────────────
  apply(analyticsGranted());

  // cookie-consent.js fires this on every write, with the record in detail.
  // Falling back to a fresh read keeps the two files from having to agree
  // on the event's shape.
  root.addEventListener('cygenix:cookie-consent', function (ev) {
    var d = ev && ev.detail;
    apply(d && typeof d.analytics === 'boolean' ? d.analytics : analyticsGranted());
  });

  // For the tests, and for anyone debugging a page in the console.
  root.CygenixGA4 = {
    id: MEASUREMENT_ID,
    isLoaded: function () { return _injected; },
    consented: analyticsGranted,
  };
})(typeof window !== 'undefined' ? window : this);
