/**
 * cookie-consent.js
 * EU/UK GDPR compliant cookie consent banner for Cygenix.
 * Add <script src="/cookie-consent.js"></script> to every page.
 * Stores preference in localStorage as cygenix_cookie_consent.
 */
(function() {
  const CONSENT_KEY = 'cygenix_cookie_consent';
  const CONSENT_VERSION = '1';

  // Check if consent already given for this version
  function getConsent() {
    try {
      const raw = localStorage.getItem(CONSENT_KEY);
      if (!raw) return null;
      return JSON.parse(raw);
    } catch { return null; }
  }

  function setConsent(essential, functional) {
    localStorage.setItem(CONSENT_KEY, JSON.stringify({
      version:   CONSENT_VERSION,
      essential: true, // always true
      functional,
      timestamp: new Date().toISOString()
    }));
  }

  function removePanel() {
    const banner = document.getElementById('cc-banner');
    const panel  = document.getElementById('cc-panel');
    if (banner) banner.remove();
    if (panel)  panel.remove();
  }

  function acceptAll() {
    setConsent(true, true);
    removePanel();
  }

  function acceptEssential() {
    setConsent(true, false);
    removePanel();
  }

  function openPanel() {
    document.getElementById('cc-banner')?.remove();
    document.getElementById('cc-panel')?.remove();
    showPanel();
  }

  function savePanel() {
    const functional = document.getElementById('cc-functional')?.checked ?? false;
    setConsent(true, functional);
    removePanel();
  }

  function showBanner() {
    const el = document.createElement('div');
    el.id = 'cc-banner';
    el.innerHTML = `
      <div style="
        position:fixed;bottom:0;left:0;right:0;z-index:9999;
        background:var(--bg2);border-top:0.5px solid var(--border2);
        padding:1rem 1.5rem;display:flex;align-items:center;justify-content:space-between;
        gap:1rem;flex-wrap:wrap;font-family:var(--serif);
        box-shadow:var(--shadow-strong)
      ">
        <div style="flex:1;min-width:240px">
          <div style="font-size:13px;font-weight:600;color:var(--text);margin-bottom:4px">🍪 Cookie preferences</div>
          <div style="font-size:12px;color:var(--text2);line-height:1.55">
            We use essential cookies to keep you logged in and save your work. We also use functional cookies to sync your data across devices.
            <a href="/privacy.html#cookies" style="color:var(--accent);text-decoration:none"> Learn more</a>
          </div>
        </div>
        <div style="display:flex;gap:0.625rem;flex-shrink:0;flex-wrap:wrap">
          <button onclick="CygenixCookies.openPanel()" style="
            padding:7px 14px;border-radius:6px;font-size:12px;font-family:inherit;font-weight:500;
            border:0.5px solid var(--border2);background:transparent;color:var(--text2);cursor:pointer
          ">Manage</button>
          <button onclick="CygenixCookies.acceptEssential()" style="
            padding:7px 14px;border-radius:6px;font-size:12px;font-family:inherit;font-weight:500;
            border:0.5px solid var(--border2);background:transparent;color:var(--text2);cursor:pointer
          ">Essential only</button>
          <button onclick="CygenixCookies.acceptAll()" style="
            padding:7px 14px;border-radius:6px;font-size:12px;font-family:inherit;font-weight:500;
            border:0.5px solid var(--accent2);background:var(--accent);color:#fff;cursor:pointer
          ">Accept all</button>
        </div>
      </div>`;
    document.body.appendChild(el);
  }

  function showPanel() {
    const consent = getConsent();
    const el = document.createElement('div');
    el.id = 'cc-panel';
    el.innerHTML = `
      <div style="
        position:fixed;inset:0;z-index:10000;background:var(--modal-scrim);
        display:flex;align-items:center;justify-content:center;padding:1rem
      " onclick="if(event.target===this)CygenixCookies.closePanel()">
        <div style="
          background:var(--bg2);border:0.5px solid var(--border2);border-radius:12px;
          padding:1.75rem;width:100%;max-width:480px;font-family:var(--serif)
        ">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:1.25rem">
            <div style="font-size:15px;font-weight:600;color:var(--text)">Cookie settings</div>
            <button onclick="CygenixCookies.closePanel()" style="
              background:transparent;border:none;color:var(--text3);font-size:18px;cursor:pointer;padding:4px;line-height:1
            ">✕</button>
          </div>

          <div style="display:flex;flex-direction:column;gap:0.875rem;margin-bottom:1.5rem">

            <div style="
              display:flex;align-items:flex-start;justify-content:space-between;gap:1rem;
              padding:12px 14px;border-radius:8px;background:var(--bg3);border:0.5px solid var(--border)
            ">
              <div>
                <div style="font-size:13px;font-weight:600;color:var(--text);margin-bottom:3px">Essential cookies</div>
                <div style="font-size:12px;color:var(--text2);line-height:1.5">Required for login, session management, and saving your work locally. Cannot be disabled.</div>
              </div>
              <div style="
                flex-shrink:0;font-size:10px;font-family:'IBM Plex Mono',monospace;
                background:var(--green-bg);border:0.5px solid var(--green);
                color:var(--green);padding:2px 8px;border-radius:100px;margin-top:2px;white-space:nowrap
              ">Always on</div>
            </div>

            <div style="
              display:flex;align-items:flex-start;justify-content:space-between;gap:1rem;
              padding:12px 14px;border-radius:8px;background:var(--bg3);border:0.5px solid var(--border)
            ">
              <div>
                <div style="font-size:13px;font-weight:600;color:var(--text);margin-bottom:3px">Functional cookies</div>
                <div style="font-size:12px;color:var(--text2);line-height:1.5">Enables syncing your migration jobs and settings across devices via Azure Cosmos DB.</div>
              </div>
              <label style="flex-shrink:0;cursor:pointer;margin-top:2px">
                <input type="checkbox" id="cc-functional" ${consent?.functional ? 'checked' : ''} style="
                  width:36px;height:20px;appearance:none;background:var(--bg4);border:0.5px solid var(--border2);
                  border-radius:10px;cursor:pointer;position:relative;transition:background 0.2s;
                  display:block
                " onchange="this.style.background=this.checked?'var(--accent)':'var(--bg4)'">
              </label>
            </div>

          </div>

          <div style="font-size:11px;color:var(--text3);margin-bottom:1.25rem;line-height:1.5">
            We do not use advertising or tracking cookies. See our
            <a href="/privacy.html#cookies" style="color:var(--accent);text-decoration:none">Privacy Policy</a> for full details.
          </div>

          <div style="display:flex;gap:0.75rem;justify-content:flex-end">
            <button onclick="CygenixCookies.acceptEssential()" style="
              padding:8px 16px;border-radius:6px;font-size:13px;font-family:inherit;font-weight:500;
              border:0.5px solid var(--border2);background:transparent;color:var(--text2);cursor:pointer
            ">Essential only</button>
            <button onclick="CygenixCookies.savePanel()" style="
              padding:8px 16px;border-radius:6px;font-size:13px;font-family:inherit;font-weight:500;
              border:0.5px solid var(--accent2);background:var(--accent);color:#fff;cursor:pointer
            ">Save preferences</button>
          </div>
        </div>
      </div>`;

    // Set initial toggle colour
    setTimeout(() => {
      const cb = document.getElementById('cc-functional');
      if (cb) cb.style.background = cb.checked ? 'var(--accent)' : 'var(--bg4)';
    }, 10);

    document.body.appendChild(el);
  }

  function closePanel() {
    document.getElementById('cc-panel')?.remove();
    // Re-show banner if no consent yet
    if (!getConsent()) showBanner();
  }

  // Add cookie settings button (bottom-left, persistent)
  function addSettingsButton() {
    const btn = document.createElement('button');
    btn.id = 'cc-settings-btn';
    btn.title = 'Cookie settings';
    btn.innerHTML = '🍪';
    btn.onclick = openPanel;
    btn.style.cssText = `
      position:fixed;bottom:1.25rem;left:1.25rem;z-index:8888;
      width:36px;height:36px;border-radius:50%;
      background:var(--bg3);border:0.5px solid var(--border2);
      font-size:16px;cursor:pointer;display:flex;align-items:center;justify-content:center;
      transition:border-color 0.15s;box-shadow:0 2px 8px rgba(0,0,0,0.4)
    `;
    btn.onmouseover = () => btn.style.borderColor = 'rgba(61,126,255,0.4)';
    btn.onmouseout  = () => btn.style.borderColor = 'var(--border2)';
    document.body.appendChild(btn);
  }

  // Public API
  window.CygenixCookies = { acceptAll, acceptEssential, openPanel, closePanel, savePanel, getConsent };

  // Init on DOM ready
  function init() {
    addSettingsButton();
    const consent = getConsent();
    if (!consent || consent.version !== CONSENT_VERSION) {
      showBanner();
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    setTimeout(init, 200);
  }
})();
