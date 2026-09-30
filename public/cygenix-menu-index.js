/* cygenix-menu-index.js — find a menu item or a single setting by name.
 *
 * WHY THIS EXISTS
 *
 * The masthead search and the Search page both searched SAVED WORK only:
 * jobs, maps, projects, artefacts, reports. Typing "claude" or "api key"
 * found nothing, because the Anthropic key is not saved work — it is one
 * field, on one panel, of a screen that lives in the account menu under the
 * name "General settings". Nobody looking for the key would guess that name,
 * and the search box, which is where people go when they cannot guess, had
 * no answer either. Settings were only findable by already knowing where
 * they were.
 *
 * This is a small local index of two kinds of thing:
 *
 *   MENU     every destination the rail, its tab strips and the account menu
 *            offer, read from cygenix-sidebar.js through
 *            CygenixSidebar.navEntries(). Generated, never copied: a new rail
 *            item is searchable the moment it exists, and an item the rail
 *            hides from this person (the Audit log, for anyone who is not an
 *            Owner, Administrator or Auditor) is hidden here by the same
 *            test, because a result that lands on a screen you cannot use is
 *            worse than no result.
 *
 *   SETTING  individual fields, kept by hand in SETTINGS below. They cannot
 *            be generated: the markup does not say which inputs are settings
 *            or what a person would call them. Every entry names the nav key
 *            of the screen it lives on and a selector for the field itself.
 *            tests/menu-index.test.js checks both against the real repo — a
 *            key the sidebar does not know, or an id that is not in the
 *            markup, fails the build rather than producing a result that
 *            goes nowhere.
 *
 * Nothing here touches the network. The index is built from data already in
 * the page, on every search; it is some seventy entries and rebuilding is
 * cheaper than keeping a cache honest when the Audit log's visibility
 * arrives asynchronously.
 *
 * LANDING ON THE FIELD, NOT JUST THE SCREEN
 *
 * go(entry) navigates the way clicking the menu item would (it asks the
 * sidebar to — handleClick is the one place that knows which keys are
 * dashboard views and which are pages) and leaves the field's selector in
 * sessionStorage.cyg_focus_target. consumeFocusTarget() runs on every page
 * load and at the end of every showView: it waits for the field to exist and
 * be visible (polling every 100ms, for up to 3s, because the Connections
 * tabs and the Collation card are built after the view appears), then
 * scrolls to it, focuses it and flashes it.
 *
 * THE ONE-SHOT RULE
 * The key is removed BEFORE the scroll, focus and flash, and never from a
 * callback those set off. A one-shot flag reset by its own callback is how
 * a render loop starts: if landing ever caused a showView, a flag still set
 * at that moment would land again, and again. Cleared first, a second call
 * finds nothing to do. On the 3s timeout it is cleared as well, so a target
 * that never appears does not wait on the next page for someone who has
 * since gone elsewhere.
 *
 * Node-requirable: the ranking and the SETTINGS table are exported for
 * tests/menu-index.test.js with no DOM.
 */
(function (root, factory) {
  'use strict';
  var api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root && root.document) {
    root.CygenixMenuIndex = api;
    api.__boot();
  }
})(typeof window !== 'undefined' ? window : this, function (root) {
  'use strict';

  var FOCUS_KEY = 'cyg_focus_target';
  var POLL_MS = 100;
  var WAIT_MS = 3000;
  var FLASH_MS = 1500;

  /* ── The settings ──────────────────────────────────────────────────────────
     One entry per field a person might come looking for. `navKey` is the
     sidebar key of the screen; `anchor` is a selector for the field, or a
     comma-separated list tried IN ORDER, the first visible one winning — the
     Connections fields exist twice over (pasted string or built from parts,
     direct or through a Function App) and only the mode in use is shown.
     `tab` is a Connections tab to open first. `keywords` are the words people
     use that the label does not.

     Every navKey and every #id here is checked against the repo by
     tests/menu-index.test.js. Candidates that were left out because their
     target does not exist are listed in that test's header. */
  var SETTINGS = [
    // General settings — #view-project-settings
    { id: 'anthropic-key', label: 'Anthropic API key (Claude)', path: 'Settings › General',
      navKey: 'project-settings', anchor: '#settings-api-key',
      keywords: ['claude', 'anthropic', 'api key', 'ai key', 'sk-ant', 'assistant key', 'llm'] },
    { id: 'primary-model', label: 'Primary model', path: 'Settings › General',
      navKey: 'project-settings', anchor: '#cygmm-primary',
      keywords: ['model', 'claude model', 'ai model', 'opus', 'sonnet', 'haiku', 'llm'] },
    { id: 'fallback-chain', label: 'Fallback chain', path: 'Settings › General',
      navKey: 'project-settings', anchor: '#cygmm-chain',
      keywords: ['fallback', 'backup model', 'model order', 'retry model'] },
    { id: 'theme', label: 'Theme', path: 'Settings › General',
      navKey: 'project-settings', anchor: '#pref-theme',
      keywords: ['dark mode', 'light mode', 'dark', 'light', 'financial', 'appearance', 'colours', 'colors'] },
    { id: 'landing-view', label: 'Default landing view', path: 'Settings › General',
      navKey: 'project-settings', anchor: '#pref-landing',
      keywords: ['start page', 'home page', 'first screen', 'landing'] },
    { id: 'report-format', label: 'Default report format', path: 'Settings › General',
      navKey: 'project-settings', anchor: '#pref-report-format',
      keywords: ['pdf', 'excel', 'export format', 'report type'] },
    { id: 'auto-snapshot', label: 'Auto-snapshot on save', path: 'Settings › General',
      navKey: 'project-settings', anchor: '#pref-auto-snapshot',
      keywords: ['snapshot', 'version', 'history', 'autosave'] },
    { id: 'backup', label: 'Backup and restore', path: 'Settings › General',
      navKey: 'project-settings', anchor: '#quick-backup-last-run',
      keywords: ['backup', 'restore', 'export', 'download everything', 'backup json'] },

    // Connections — #view-connections, Database connections tab
    { id: 'source-connection', label: 'Source connection string', path: 'Connect › Connections',
      navKey: 'connections', tab: 'databases', anchor: '#proj-src-cs, #src-b-host, #proj-src-fn-url, #src-conn-panel',
      keywords: ['source', 'connection string', 'server', 'database', 'host', 'sql server', 'postgres'] },
    { id: 'target-connection', label: 'Target connection string', path: 'Connect › Connections',
      navKey: 'connections', tab: 'databases', anchor: '#proj-tgt-cs, #tgt-b-host, #proj-tgt-fn-url, #tgt-conn-panel',
      keywords: ['target', 'destination', 'connection string', 'server', 'database', 'host'] },
    { id: 'function-url', label: 'Azure Function URL', path: 'Connect › Connections',
      navKey: 'connections', tab: 'databases', anchor: '#proj-src-fn-url, #proj-tgt-fn-url, #src-conn-panel',
      keywords: ['azure function', 'function app', 'endpoint', 'relay', 'url'] },
    { id: 'function-key', label: 'Azure Function key', path: 'Connect › Connections',
      navKey: 'connections', tab: 'databases', anchor: '#proj-src-fn-key, #proj-tgt-fn-key, #src-conn-panel',
      keywords: ['function key', 'host key', 'code', 'access key'] },
    { id: 'collation', label: 'Collation settings', path: 'Connect › Connections',
      navKey: 'connections', tab: 'databases', anchor: '#cyg-collation-card',
      keywords: ['collation', 'case sensitive', 'accent sensitive', 'sort order', 'ci as', 'latin1'] },

    // Notifications — #view-notifications
    { id: 'notify-events', label: 'Run notification events', path: 'Settings › Notifications',
      navKey: 'notifications', anchor: '#nt-events',
      keywords: ['alerts', 'notify', 'job failed', 'job completed', 'events'] },
    { id: 'notify-delivery', label: 'Email and webhook delivery', path: 'Settings › Notifications',
      navKey: 'notifications', anchor: '#nt-connectors',
      keywords: ['email', 'smtp', 'resend', 'webhook', 'where notifications go'] },
    { id: 'notify-server', label: 'Notifications for scheduled runs', path: 'Settings › Notifications',
      navKey: 'notifications', anchor: '#nt-server',
      keywords: ['scheduled', 'server delivery', 'email', 'resend', 'unattended'] },

    // Governance — #view-privacy-security
    { id: 'ai-access-mode', label: 'AI access mode', path: 'Settings › Governance',
      navKey: 'privacy-security', anchor: '#ps-mode-indicator',
      keywords: ['ai access', 'privacy', 'data sent to ai', 'metadata only'] },
    { id: 'value-redaction', label: 'Value redaction', path: 'Settings › Governance',
      navKey: 'privacy-security', anchor: '#ps-tog-redact',
      keywords: ['redact', 'mask', 'pii', 'privacy', 'personal data'] },
    { id: 'column-exclusion', label: 'Sensitive column exclusion', path: 'Settings › Governance',
      navKey: 'privacy-security', anchor: '#ps-new-pattern',
      keywords: ['exclude columns', 'sensitive', 'pii', 'salary', 'ssn', 'privacy'] },

    // Subscription — the account menu's item, which opens the billing portal
    // for a paying account and the plan picker otherwise.
    { id: 'subscription', label: 'Subscription and billing', path: 'Account',
      billing: true, href: '/pick-plan',
      keywords: ['billing', 'plan', 'tier', 'upgrade', 'invoice', 'payment', 'pricing'] },
  ];

  /* Words for whole screens. These screens are menu items already — a
     second "Setting" row for "Users & roles" would only be a duplicate — so
     their extra words attach to the menu entry instead. */
  var MENU_KEYWORDS = {
    'project-settings':  ['settings', 'preferences', 'general', 'api key', 'claude', 'model'],
    'notifications':     ['alerts', 'email', 'resend', 'smtp', 'webhook'],
    'system-parameters': ['parameters', 'variables', 'config', 'what-if', 'wasis'],
    'user-roles':        ['users', 'roles', 'rbac', 'permissions', 'access', 'invite', 'team'],
    'privacy-security':  ['governance', 'privacy', 'security', 'redaction', 'pii'],
    'accessibility':     ['a11y', 'text size', 'contrast', 'motion', 'screen reader'],
    'help':              ['help', 'guide', 'docs', 'documentation', 'how to'],
    'connections':       ['connection string', 'source', 'target', 'database', 'server'],
    'profiles':          ['profiles', 'environments', 'dev', 'test', 'prod', 'saved connections'],
    'integrations':      ['integrations', 'webhook', 'smtp', 'slack', 'teams', 'jira', 'github', 'azure devops', 'blob', 'sharepoint', 'fivetran', 'airbyte', 'dbt'],
    'diagnostics':       ['diagnostics', 'troubleshoot', 'probe', 'health check', 'debug'],
    'audit':             ['audit', 'trail', 'log', 'history', 'who did what'],
    'task-agent':        ['schedule', 'cron', 'task agent', 'chain'],
    'object-mapping':    ['mapping', 'map columns', 'ai mapping'],
    'sql-editor':        ['sql', 'query', 'develop'],
    'claude-code':       ['dev console', 'claude code', 'claude', 'code', 'python', 'agent', 'script', 'workspace', 'develop', 'console'],
  };

  /* ── Ranking ───────────────────────────────────────────────────────────────
     No fuzzy library: a person typing a setting's name wants that setting,
     and fuzzy matching's cost is results that are near a word rather than
     about it. Five tiers, best first:

       500  the label IS the query
       400  the label starts with the query
       300  every query word starts a word of the label
       200  every query word starts a word of the keywords
       100  every query word appears somewhere — label, breadcrumb, keywords

     Case is ignored; so are &, ›, hyphens and every other punctuation mark,
     so "users & roles", "users roles" and "Users&Roles" are the same query.
     A multi-word query must match ALL its words. Ties go to the shorter
     label, then to the order the entries were declared in. */
  function normalize(s) {
    return String(s == null ? '' : s).toLowerCase()
      .replace(/[^\p{L}\p{N}]+/gu, ' ').trim();
  }
  function words(s) { var n = normalize(s); return n ? n.split(' ') : []; }
  function everyStarts(qw, ws) {
    return qw.every(function (q) { return ws.some(function (w) { return w.indexOf(q) === 0; }); });
  }

  function score(entry, q) {
    var Q = normalize(q);
    if (!Q) return 0;
    var qw = Q.split(' ');
    var L = normalize(entry.label);
    if (L === Q) return 500;
    if (L.indexOf(Q) === 0) return 400;
    if (everyStarts(qw, L ? L.split(' ') : [])) return 300;
    var kw = [];
    (entry.keywords || []).forEach(function (k) { kw = kw.concat(words(k)); });
    if (kw.length && everyStarts(qw, kw)) return 200;
    var hay = [L, normalize(entry.path), normalize(entry.parentLabel), kw.join(' ')].join(' ');
    if (qw.every(function (w) { return hay.indexOf(w) !== -1; })) return 100;
    return 0;
  }

  function rank(entries, q, limit) {
    var scored = [];
    (entries || []).forEach(function (e, i) {
      var s = score(e, q);
      if (s > 0) scored.push({ e: e, s: s, i: i });
    });
    scored.sort(function (a, b) {
      return (b.s - a.s)
        || (normalize(a.e.label).length - normalize(b.e.label).length)
        || (a.i - b.i);
    });
    var out = scored.map(function (x) { return x.e; });
    return typeof limit === 'number' && limit >= 0 ? out.slice(0, limit) : out;
  }

  /* ── Building the index ────────────────────────────────────────────────────
     `nav` is CygenixSidebar.navEntries(): already filtered for what this
     person may see. A setting is offered only when its screen is: if the
     sidebar would not show the screen, the index does not show a field on
     it. Without the sidebar (it failed to load) there is no menu to check
     against, and settings are offered as they are — the dropdown that would
     show them is the sidebar's, so in practice that is the Search page only. */
  function buildFrom(nav) {
    var haveNav = Array.isArray(nav);
    var visible = {};
    var menu = (nav || []).map(function (n) {
      visible[n.key] = true;
      var extra = MENU_KEYWORDS[n.key] || [];
      return {
        id: 'menu:' + n.key, kind: 'menu', key: n.key, navKey: n.key,
        label: n.label,
        path: [n.section, n.parentLabel].filter(Boolean).join(' › ') || 'Menu',
        parentLabel: n.parentLabel || '',
        view: n.view, href: n.href, action: n.action,
        keywords: (n.keywords || []).concat(extra),
      };
    });
    var settings = SETTINGS.filter(function (s) {
      return !s.navKey || !haveNav || visible[s.navKey];
    }).map(function (s) {
      return {
        id: 'set:' + s.id, kind: 'setting', navKey: s.navKey || '',
        label: s.label, path: s.path, parentLabel: '',
        anchor: s.anchor || '', tab: s.tab || '', href: s.href || '', billing: !!s.billing,
        keywords: s.keywords || [],
      };
    });
    return menu.concat(settings);
  }

  function sidebar() { return root && root.CygenixSidebar; }

  function build() {
    var sb = sidebar();
    var nav = null;
    try { if (sb && typeof sb.navEntries === 'function') nav = sb.navEntries(); } catch (e) { nav = null; }
    return buildFrom(nav);
  }

  function search(q, limit) {
    return rank(build(), q, typeof limit === 'number' ? limit : 10);
  }

  function byId(id) {
    var all = build();
    for (var i = 0; i < all.length; i++) if (all[i].id === id) return all[i];
    return null;
  }

  /* ── Going there ─────────────────────────────────────────────────────────── */
  function here() {
    return (root.location.pathname || '').replace(/\.html$/, '').replace(/\/+$/, '') || '/';
  }
  function setFocusTarget(sel) {
    // The key is spelled out, not passed as FOCUS_KEY, in all three calls:
    // scripts/storage-inventory.js finds keys by their literal, and one it
    // cannot see is one docs/storage-inventory.md does not classify.
    try { if (sel) root.sessionStorage.setItem('cyg_focus_target', sel); } catch (e) { /* private window: land on the screen only */ }
  }

  function go(entry) {
    if (!entry) return false;
    if (entry.billing) {
      if (typeof root.openBillingPortal === 'function') {
        try { root.openBillingPortal(); return true; } catch (e) { /* fall through to the plan picker */ }
      }
      root.location.href = entry.href || '/pick-plan';
      return true;
    }
    if (entry.anchor) setFocusTarget(entry.anchor);

    var sb = sidebar();
    var key = entry.navKey || entry.key;
    var item = sb && typeof sb.findItem === 'function' ? sb.findItem(key) : null;
    var onDashboard = here() === '/dashboard';

    // A Connections field on another page: go straight to the tab, through
    // the same goto= reader the dashboard already has for "connections/import".
    if (entry.tab && item && item.view && !onDashboard) {
      var target = item.view + '/' + entry.tab;
      try { root.sessionStorage.setItem('cyg_goto', target); } catch (e) {}
      root.location.href = '/dashboard#goto=' + encodeURIComponent(target);
      return true;
    }

    var moved = false;
    if (sb && typeof sb.navigate === 'function') moved = sb.navigate(key);
    if (!moved) {
      // No sidebar: the two shapes a destination can have, done by hand.
      if (entry.view) root.location.href = '/dashboard#goto=' + encodeURIComponent(entry.view);
      else if (entry.href) root.location.href = entry.href;
      else return false;
    }
    if (entry.tab && onDashboard && typeof root.switchConnTab === 'function') {
      try { root.switchConnTab(entry.tab); } catch (e) {}
    }
    // Staying on this page (a dashboard view, switched in place): look for
    // the field now. Leaving it: do NOT — this page does not have the field,
    // and a wait started here could time out and clear the key before the
    // next page loads.
    var staying = !!(item && item.view && onDashboard);
    if (staying) consumeFocusTarget();
    return true;
  }

  function goId(id) { return go(byId(id)); }

  /* ── Landing ─────────────────────────────────────────────────────────────── */
  function isVisible(el) {
    if (!el || !el.isConnected) return false;
    var cs = null;
    try { cs = root.getComputedStyle(el); } catch (e) {}
    if (cs && (cs.display === 'none' || cs.visibility === 'hidden')) return false;
    if (el.offsetParent === null && !(cs && cs.position === 'fixed')) return false;
    var r = el.getBoundingClientRect();
    return r.width > 0 || r.height > 0;
  }
  function firstVisible(sel) {
    var parts = String(sel).split(',');
    for (var i = 0; i < parts.length; i++) {
      var s = parts[i].trim();
      if (!s) continue;
      var el = null;
      try { el = root.document.querySelector(s); } catch (e) { el = null; }
      if (isVisible(el)) return el;
    }
    return null;
  }
  var FOCUSABLE = 'input:not([type=hidden]),select,textarea,button,a[href],[tabindex]:not([tabindex="-1"])';
  function focusTarget(el) {
    if (el.matches && el.matches(FOCUSABLE)) return el;
    var inner = el.querySelectorAll ? el.querySelectorAll(FOCUSABLE) : [];
    for (var i = 0; i < inner.length; i++) if (isVisible(inner[i]) && !inner[i].disabled) return inner[i];
    return null;
  }

  function injectFlashStyle() {
    var d = root.document;
    if (!d || d.getElementById('cyg-search-flash-style')) return;
    var st = d.createElement('style');
    st.id = 'cyg-search-flash-style';
    // The accent token, so a theme that remaps it (financial) moves the
    // outline with it. It pulses from a wide soft ring to a tight one and
    // stays for the full 1.5s, so a person looking elsewhere at the moment
    // of arrival still sees where they landed. No motion if they asked for
    // none — the outline simply holds.
    st.textContent =
      '.cyg-search-flash{outline:2px solid var(--color-accent,#5980a6)!important;outline-offset:3px;' +
      'animation:cygSearchFlash 1.5s ease-out 1}' +
      '@keyframes cygSearchFlash{0%{box-shadow:0 0 0 8px color-mix(in srgb,var(--color-accent,#5980a6) 35%,transparent)}' +
      '60%{box-shadow:0 0 0 3px color-mix(in srgb,var(--color-accent,#5980a6) 20%,transparent)}' +
      '100%{box-shadow:0 0 0 0 transparent}}' +
      '@media (prefers-reduced-motion: reduce){.cyg-search-flash{animation:none}}';
    (d.head || d.documentElement).appendChild(st);
  }

  function land(el) {
    try { el.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) { try { el.scrollIntoView(); } catch (_) {} }
    var f = focusTarget(el);
    if (f) { try { f.focus({ preventScroll: true }); } catch (e) { try { f.focus(); } catch (_) {} } }
    injectFlashStyle();
    el.classList.add('cyg-search-flash');
    // Removes the class only. It does not read or write the key, and it
    // cannot cause a showView, so it cannot start another landing.
    setTimeout(function () { el.classList.remove('cyg-search-flash'); }, FLASH_MS);
  }

  var waiting = null;
  function readKey() { try { return root.sessionStorage.getItem('cyg_focus_target') || ''; } catch (e) { return ''; } }
  function clearKey() { try { root.sessionStorage.removeItem('cyg_focus_target'); } catch (e) {} }

  function consumeFocusTarget() {
    // One wait at a time. The one already running re-reads the key on every
    // tick, so a newer target set while it waits is the one it lands on.
    if (waiting) return;
    if (!readKey()) return;
    var started = Date.now();
    function tick() {
      waiting = null;
      var sel = readKey();
      if (!sel) return;
      var el = firstVisible(sel);
      if (el) { clearKey(); land(el); return; }
      if (Date.now() - started >= WAIT_MS) { clearKey(); return; }
      waiting = setTimeout(tick, POLL_MS);
    }
    // The FIRST look is on the next tick, never inside the call that asked.
    // showView calls this, and whoever called showView may not be finished:
    // go() switches Connections to its Database tab straight afterwards, and
    // the Collation module rebuilds its card with innerHTML on every visit to
    // that tab. Landing synchronously flashed and focused a card that was
    // replaced a moment later — measured, not guessed: the card was there
    // and visible, and neither the flash nor the focus was. After the
    // current task, the element found is the one that stays.
    waiting = setTimeout(tick, 0);
  }

  function boot() {
    var d = root.document;
    if (d.readyState === 'loading') d.addEventListener('DOMContentLoaded', consumeFocusTarget);
    else consumeFocusTarget();
  }

  return {
    build: build, search: search, go: go, goId: goId, byId: byId,
    consumeFocusTarget: consumeFocusTarget,
    SETTINGS: SETTINGS, MENU_KEYWORDS: MENU_KEYWORDS, FOCUS_KEY: FOCUS_KEY,
    normalize: normalize, score: score, rank: rank, buildFrom: buildFrom,
    __boot: boot,
  };
});
