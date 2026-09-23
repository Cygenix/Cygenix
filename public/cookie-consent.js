/**
 * cookie-consent.js
 * EU/UK GDPR compliant cookie consent banner for Cygenix.
 * Add <script src="/cookie-consent.js"></script> to every page.
 * Stores preference in localStorage as cygenix_cookie_consent.
 *
 * ── VERSION 2 (Sep-2026): the analytics category ─────────────────────────
 *
 * The public marketing pages now carry Google Analytics 4, so the record
 * gained a third category and the version went to '2'. The bump is not
 * bookkeeping: a consent record collected when the only categories were
 * essential and functional does not cover measurement by a third party,
 * so everyone is asked once more. Somebody who previously chose
 * "Essential only" is not silently opted in, and somebody who chose
 * "Accept all" is not assumed to have accepted a purpose that did not
 * exist when they clicked.
 *
 * WHO OWNS WHAT
 * This file owns the decision and the record. cygenix-ga4.js owns the tag
 * and loads Google only when this file says analytics is granted, which it
 * announces by dispatching 'cygenix:cookie-consent' on every write with
 * the record as detail. Nothing here knows what a measurement id is.
 *
 * EQUAL PROMINENCE
 * Reject and Accept are rendered with identical styling — same size, same
 * weight, same border, same background. The ICO's position is that
 * refusing must be as easy as accepting, and a greyed-out "Essential only"
 * beside a filled blue "Accept all" is the pattern it objects to. Manage
 * sits apart as a quieter third option because it opens a panel rather
 * than making a choice.
 *
 * VAR FALLBACKS
 * Every custom property carries a literal fallback. The banner is shown on
 * marketing pages that define the console's colour tokens and on two that
 * do not (about, register); without fallbacks it rendered there with a
 * transparent background and invisible text.
 */
(function() {
  const CONSENT_KEY = 'cygenix_cookie_consent';
  const CONSENT_VERSION = '2';

  // Colour tokens with literal fallbacks, so the banner is legible on a
  // page that defines none of them.
  const C = {
    bg2:    'var(--bg2, #FFFFFF)',
    bg3:    'var(--bg3, #ECEEF0)',
    bg4:    'var(--bg4, #E1E4E7)',
    text:   'var(--text, #1A1D21)',
    text2:  'var(--text2, #565B63)',
    text3:  'var(--text3, #363D49)',
    border: 'var(--border, rgba(22,26,32,0.12))',
    border2:'var(--border2, rgba(22,26,32,0.20))',
    accent: 'var(--accent, #4A5BD6)',
    accent2:'var(--accent2, #3D4EC4)',
    green:  'var(--green, #3F7D4E)',
    greenBg:'var(--green-bg, rgba(63,125,78,0.12))',
    serif:  'var(--serif, ui-serif, Georgia, serif)',
    shadow: 'var(--shadow-strong, 0 -2px 16px rgba(0,0,0,0.18))',
    scrim:  'var(--modal-scrim, rgba(0,0,0,0.45))',
  };

  // The two choice buttons share one style string. They differ in their
  // label and in nothing else, which is the point.
  const CHOICE_BTN = `
    padding:7px 16px;border-radius:6px;font-size:12px;font-family:inherit;font-weight:500;
    border:0.5px solid ${C.border2};background:${C.bg3};color:${C.text};cursor:pointer;
    min-width:88px
  `;
  const QUIET_BTN = `
    padding:7px 14px;border-radius:6px;font-size:12px;font-family:inherit;font-weight:500;
    border:none;background:transparent;color:${C.text2};cursor:pointer;text-decoration:underline
  `;

  // Check if consent already given for this version
  function getConsent() {
    try {
      const raw = localStorage.getItem(CONSENT_KEY);
      if (!raw) return null;
      return JSON.parse(raw);
    } catch { return null; }
  }

  // Every write goes through here, so every write announces itself. The
  // storage write is wrapped: a private window must still be able to
  // dismiss the banner and still get the analytics decision it asked for,
  // even when the choice cannot be remembered for next time.
  function setConsent(essential, functional, analytics) {
    const record = {
      version:   CONSENT_VERSION,
      essential: true, // always true
      functional,
      analytics,
      timestamp: new Date().toISOString()
    };
    try {
      localStorage.setItem(CONSENT_KEY, JSON.stringify(record));
    } catch (e) { /* choice still applies to this page view */ }
    try {
      window.dispatchEvent(new CustomEvent('cygenix:cookie-consent', { detail: record }));
    } catch (e) { /* no CustomEvent: cygenix-ga4.js re-reads on next load */ }
    return record;
  }

  function removePanel() {
    const banner = document.getElementById('cc-banner');
    const panel  = document.getElementById('cc-panel');
    if (banner) banner.remove();
    if (panel)  panel.remove();
  }

  function acceptAll() {
    setConsent(true, true, true);
    removePanel();
  }

  // Everything off but the essentials. This is what Reject does, and it is
  // also what the panel's "Essential only" does — one path, so the two
  // cannot drift apart.
  function acceptEssential() {
    setConsent(true, false, false);
    removePanel();
  }
  const rejectAll = acceptEssential;

  function openPanel() {
    document.getElementById('cc-banner')?.remove();
    document.getElementById('cc-panel')?.remove();
    showPanel();
  }

  function savePanel() {
    const functional = document.getElementById('cc-functional')?.checked ?? false;
    const analytics  = document.getElementById('cc-analytics')?.checked ?? false;
    setConsent(true, functional, analytics);
    removePanel();
  }

  function showBanner() {
    const el = document.createElement('div');
    el.id = 'cc-banner';
    el.setAttribute('role', 'region');
    el.setAttribute('aria-label', 'Cookie preferences');
    el.innerHTML = `
      <div style="
        position:fixed;bottom:0;left:0;right:0;z-index:9999;
        background:${C.bg2};border-top:0.5px solid ${C.border2};
        padding:1rem 1.5rem;display:flex;align-items:center;justify-content:space-between;
        gap:1rem;flex-wrap:wrap;font-family:${C.serif};
        box-shadow:${C.shadow}
      ">
        <div style="flex:1;min-width:240px">
          <div style="font-size:13px;font-weight:600;color:${C.text};margin-bottom:4px"><i class="ic ic-info"></i> Cookie preferences</div>
          <div style="font-size:12px;color:${C.text2};line-height:1.55">
            We use essential cookies to keep you logged in and save your work. With your permission we also use
            functional cookies to sync your data across devices, and Google Analytics to count visits to our public pages.
            <a href="/privacy#cookies" style="color:${C.accent};text-decoration:none"> Learn more</a>
          </div>
        </div>
        <div style="display:flex;gap:0.625rem;flex-shrink:0;flex-wrap:wrap;align-items:center">
          <button onclick="CygenixCookies.openPanel()" style="${QUIET_BTN}">Manage</button>
          <button onclick="CygenixCookies.rejectAll()" style="${CHOICE_BTN}">Reject</button>
          <button onclick="CygenixCookies.acceptAll()" style="${CHOICE_BTN}">Accept</button>
        </div>
      </div>`;
    document.body.appendChild(el);
  }

  function toggleRow(id, checked) {
    return `
      <label style="flex-shrink:0;cursor:pointer;margin-top:2px">
        <input type="checkbox" id="${id}" ${checked ? 'checked' : ''} style="
          width:36px;height:20px;appearance:none;background:${C.bg4};border:0.5px solid ${C.border2};
          border-radius:10px;cursor:pointer;position:relative;transition:background 0.2s;
          display:block
        " onchange="this.style.background=this.checked?'${C.accent}':'${C.bg4}'">
      </label>`;
  }

  function showPanel() {
    const consent = getConsent();
    // A version-1 record has no analytics field. Default it to off rather
    // than to the old "accept all" answer: the previous yes was to a
    // shorter list of purposes.
    const el = document.createElement('div');
    el.id = 'cc-panel';
    el.innerHTML = `
      <div style="
        position:fixed;inset:0;z-index:10000;background:${C.scrim};
        display:flex;align-items:center;justify-content:center;padding:1rem
      " onclick="if(event.target===this)CygenixCookies.closePanel()">
        <div style="
          background:${C.bg2};border:0.5px solid ${C.border2};border-radius:12px;
          padding:1.75rem;width:100%;max-width:480px;font-family:${C.serif}
        ">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:1.25rem">
            <div style="font-size:15px;font-weight:600;color:${C.text}">Cookie settings</div>
            <button onclick="CygenixCookies.closePanel()" aria-label="Close" style="
              background:transparent;border:none;color:${C.text3};font-size:18px;cursor:pointer;padding:4px;line-height:1
            ">✕</button>
          </div>

          <div style="display:flex;flex-direction:column;gap:0.875rem;margin-bottom:1.5rem">

            <div style="
              display:flex;align-items:flex-start;justify-content:space-between;gap:1rem;
              padding:12px 14px;border-radius:8px;background:${C.bg3};border:0.5px solid ${C.border}
            ">
              <div>
                <div style="font-size:13px;font-weight:600;color:${C.text};margin-bottom:3px">Essential cookies</div>
                <div style="font-size:12px;color:${C.text2};line-height:1.5">Required for login, session management, and saving your work locally. Cannot be disabled.</div>
              </div>
              <div style="
                flex-shrink:0;font-size:10px;font-family:'IBM Plex Mono',monospace;
                background:${C.greenBg};border:0.5px solid ${C.green};
                color:${C.green};padding:2px 8px;border-radius:100px;margin-top:2px;white-space:nowrap
              ">Always on</div>
            </div>

            <div style="
              display:flex;align-items:flex-start;justify-content:space-between;gap:1rem;
              padding:12px 14px;border-radius:8px;background:${C.bg3};border:0.5px solid ${C.border}
            ">
              <div>
                <div style="font-size:13px;font-weight:600;color:${C.text};margin-bottom:3px">Functional cookies</div>
                <div style="font-size:12px;color:${C.text2};line-height:1.5">Enables syncing your migration jobs and settings across devices via Azure Cosmos DB.</div>
              </div>
              ${toggleRow('cc-functional', !!consent?.functional)}
            </div>

            <div style="
              display:flex;align-items:flex-start;justify-content:space-between;gap:1rem;
              padding:12px 14px;border-radius:8px;background:${C.bg3};border:0.5px solid ${C.border}
            ">
              <div>
                <div style="font-size:13px;font-weight:600;color:${C.text};margin-bottom:3px">Analytics cookies</div>
                <div style="font-size:12px;color:${C.text2};line-height:1.5">Google Analytics, on our public pages only, to count visits and see which pages people read. Never used on your migration console, and never linked to your account.</div>
              </div>
              ${toggleRow('cc-analytics', consent?.analytics === true)}
            </div>

          </div>

          <div style="font-size:11px;color:${C.text3};margin-bottom:1.25rem;line-height:1.5">
            We do not use advertising cookies or tracking pixels. See our
            <a href="/privacy#cookies" style="color:${C.accent};text-decoration:none">Privacy Policy</a> for full details.
          </div>

          <div style="display:flex;gap:0.75rem;justify-content:flex-end">
            <button onclick="CygenixCookies.rejectAll()" style="
              padding:8px 16px;border-radius:6px;font-size:13px;font-family:inherit;font-weight:500;
              border:0.5px solid ${C.border2};background:${C.bg3};color:${C.text};cursor:pointer
            ">Essential only</button>
            <button onclick="CygenixCookies.savePanel()" style="
              padding:8px 16px;border-radius:6px;font-size:13px;font-family:inherit;font-weight:500;
              border:0.5px solid ${C.accent2};background:${C.accent};color:#fff;cursor:pointer
            ">Save preferences</button>
          </div>
        </div>
      </div>`;

    // Set initial toggle colour
    setTimeout(() => {
      ['cc-functional', 'cc-analytics'].forEach((id) => {
        const cb = document.getElementById(id);
        if (cb) cb.style.background = cb.checked ? 'var(--accent, #4A5BD6)' : 'var(--bg4, #E1E4E7)';
      });
    }, 10);

    document.body.appendChild(el);
  }

  function closePanel() {
    document.getElementById('cc-panel')?.remove();
    // Re-show banner if no consent yet
    if (!currentConsent()) showBanner();
  }

  // The stored record, but only if it answers the CURRENT set of
  // categories. An older version is treated as no answer at all.
  function currentConsent() {
    const c = getConsent();
    return c && c.version === CONSENT_VERSION ? c : null;
  }

  // Add cookie settings button (bottom-left, persistent)
  function addSettingsButton() {
    const btn = document.createElement('button');
    btn.id = 'cc-settings-btn';
    btn.title = 'Cookie settings';
    btn.innerHTML = '';
    btn.onclick = openPanel;
    btn.style.cssText = `
      position:fixed;bottom:1.25rem;right:1.25rem;z-index:8888;
      width:36px;height:36px;border-radius:50%;
      background:${C.bg3};border:0.5px solid ${C.border2};
      font-size:16px;cursor:pointer;display:flex;align-items:center;justify-content:center;
      transition:border-color 0.15s;box-shadow:var(--shadow-soft,0 2px 8px rgba(0,0,0,0.2))
    `;
    btn.onmouseover = () => btn.style.borderColor = 'rgba(61,126,255,0.4)';
    btn.onmouseout  = () => btn.style.borderColor = C.border2;
    document.body.appendChild(btn);
  }

  // Public API. rejectAll is the new name; acceptEssential is kept because
  // pages have it in inline handlers and it means the same thing.
  window.CygenixCookies = { acceptAll, acceptEssential, rejectAll, openPanel, closePanel, savePanel, getConsent };

  // Init on DOM ready
  function init() {
    addSettingsButton();
    if (!currentConsent()) {
      showBanner();
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    setTimeout(init, 200);
  }
})();
