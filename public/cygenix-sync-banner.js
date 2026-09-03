/* ============================================================================
   cygenix-sync-banner.js — the part the user can actually see.
   ----------------------------------------------------------------------------
   WHY THIS EXISTS

   CYGENIX_DATA_FN_KEY was missing from the Netlify deployment for about three
   weeks. Every call to /api/data returned 503. Every save silently did
   nothing. The only trace anywhere was a console.warn, and nobody reads a
   console while they work.

   All the hardening around this — classified errors, a guarded apply, a save
   queue that survives a failure — is instrumentation. Instrumentation with no
   display is how the outage lasted three weeks rather than an afternoon. This
   is the display.

   WHAT IT SAYS, AND WHY IN THAT ORDER

   Three facts, most alarming first:

     1. Your work is not being saved.       ← the consequence
     2. Because <reason>.                    ← the cause, in the user's terms
     3. <n> changes are held on this device. ← the exposure

   Then two things to do about it: download the unsaved work, and retry. The
   download matters most. If this device's localStorage is the only copy of
   several hours of work, the single most useful control is the one that gets
   it onto disk before anything else can go wrong.

   WHAT IT DOES NOT DO

   It does not block the page. The data layer being down does not stop anyone
   editing a mapping or writing SQL — it stops that work being kept, and the
   right response is to tell them clearly, not to lock them out. An earlier
   draft of this work gated every page load behind a probe; on a healthy
   deployment that probe returned 401 and it would have blocked everybody.
   ========================================================================== */
(function () {
  'use strict';

  var BAR_ID = 'cygenix-sync-bar';

  // How to say each failure code to somebody who did not write the sync
  // layer. 'config' is the outage: it is nobody-on-this-side's fault and no
  // amount of retrying fixes it, so it says who can.
  var REASONS = {
    config: 'this deployment is missing its data configuration — an administrator needs to set it',
    auth: 'your session has expired',
    network: 'this device is offline or the connection dropped',
    server: 'the server is not responding',
    rejected: 'the server refused the change',
    'suspect-empty': 'the cloud returned an empty account while this device holds data, so nothing was overwritten',
    'no-data-api': 'the data layer did not load on this page',
  };

  function reasonText(code) {
    return REASONS[code] || 'the connection to your saved data is not working';
  }

  function el(tag, attrs, text) {
    var n = document.createElement(tag);
    Object.keys(attrs || {}).forEach(function (k) {
      if (k === 'style') n.setAttribute('style', attrs[k]);
      else n.setAttribute(k, attrs[k]);
    });
    if (text) n.textContent = text;
    return n;
  }

  function remove() {
    var b = document.getElementById(BAR_ID);
    if (b && b.parentNode) b.parentNode.removeChild(b);
  }

  function render(h) {
    // Nothing wrong, or nothing at stake: no bar. A bar that appears when
    // everything is fine is a bar people learn to ignore.
    if (!h || (!h.degraded && !h.pendingSaves)) { remove(); return; }
    // Degraded but with nothing unsaved yet is still worth saying — it means
    // the next thing they type will not be kept.
    if (!document.body) return;

    var existing = document.getElementById(BAR_ID);
    if (existing) existing.parentNode.removeChild(existing);

    var bar = el('div', {
      id: BAR_ID,
      role: 'status',
      'aria-live': 'polite',
      style: [
        'position:fixed', 'left:0', 'right:0', 'bottom:0', 'z-index:2147483000',
        'display:flex', 'flex-wrap:wrap', 'align-items:center', 'gap:10px',
        'padding:10px 16px',
        'background:#3b1f14', 'color:#ffe9df',
        'border-top:2px solid #d4713f',
        "font:500 13px/1.45 'IBM Plex Sans','Helvetica Neue',Arial,sans-serif",
        'box-shadow:0 -2px 12px rgba(0,0,0,0.28)',
      ].join(';'),
    });

    var msg = el('div', { style: 'flex:1 1 320px;min-width:0' });

    var head = el('strong', { style: 'display:block;font-weight:600' },
      h.degraded ? 'Your work is not being saved to the cloud.'
                 : 'Changes are waiting to be saved.');
    msg.appendChild(head);

    var detail = h.degraded ? 'Because ' + reasonText(h.reason) + '.' : '';
    if (h.pendingSaves) {
      detail += (detail ? ' ' : '')
        + h.pendingSaves + ' change' + (h.pendingSaves === 1 ? '' : 's')
        + ' ' + (h.pendingSaves === 1 ? 'is' : 'are') + ' held on this device only.';
    }
    msg.appendChild(el('span', { style: 'opacity:0.9' }, detail));
    bar.appendChild(msg);

    // Download first. If this browser is the only copy, this is the control
    // that matters — before Retry, before dismissing.
    var dl = el('button', { type: 'button', style: btn(true) }, 'Download my data');
    dl.addEventListener('click', function () {
      try { window.CygenixSync.exportBackup(); }
      catch (e) { console.error('[CygenixSync] export failed:', e.message); }
    });
    bar.appendChild(dl);

    var retry = el('button', { type: 'button', style: btn(false) }, 'Retry now');
    retry.addEventListener('click', function () {
      retry.disabled = true;
      retry.textContent = 'Retrying…';
      Promise.resolve(window.CygenixSync.retryPending()).catch(function () {})
        .then(function () { retry.disabled = false; retry.textContent = 'Retry now'; });
    });
    bar.appendChild(retry);

    // Dismiss hides the bar for this page view only. It deliberately does not
    // persist: the situation is still true on the next page, and a dismissal
    // that outlives the problem is how a warning becomes furniture.
    var close = el('button', {
      type: 'button', 'aria-label': 'Hide this message',
      style: btn(false) + ';padding:6px 10px',
    }, '×');
    close.addEventListener('click', remove);
    bar.appendChild(close);

    document.body.appendChild(bar);
  }

  function btn(primary) {
    return [
      'flex:0 0 auto', 'cursor:pointer',
      'padding:7px 13px', 'border-radius:8px',
      'font:600 12.5px/1 \'IBM Plex Sans\',\'Helvetica Neue\',Arial,sans-serif',
      primary ? 'background:#ffe9df' : 'background:transparent',
      primary ? 'color:#3b1f14' : 'color:#ffe9df',
      primary ? 'border:1px solid #ffe9df' : 'border:1px solid rgba(255,233,223,0.5)',
    ].join(';');
  }

  window.addEventListener('cygenix-sync-health', function (e) { render(e.detail); });

  // The sync module may have settled before this file ran — script order
  // across twenty pages is not something to depend on. Ask once on load.
  function poll() {
    try {
      if (window.CygenixSync && window.CygenixSync.getHealth) render(window.CygenixSync.getHealth());
    } catch (e) { /* sync not present on this page */ }
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', poll);
  else poll();
})();
