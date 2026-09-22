// audit-app.js — the Audit Log screen (#view-audit).
//
// Renders into #audit-log-wrap, which dashboard-app.js's renderAuditLog()
// hands over. Everything it shows comes from /.netlify/functions/audit; this
// file holds no derived truth of its own, because a screen that computes its
// own version of "how many PROD changes were there" would eventually
// disagree with the trail it is supposed to be showing.
//
// ── What replaced what ────────────────────────────────────────────────────
//
// This view used to be two tables: a server trail of the last fifty
// RBAC-gated acts, and beneath it a "browser (legacy)" list that lived in a
// JavaScript array, held a formatted time and a sentence, and was gone on
// reload. The second one is deleted rather than kept alongside, because a
// per-machine log that disappears when you refresh is not evidence of
// anything, and having it next to a hash-chained trail invited the reader to
// treat the two as equivalent. Its nine call sites now record structured
// events through CygenixAudit instead, and they survive.
//
// The old footer that sent people to Users & Roles for "the full trail,
// verification and export" is gone too. This page is the full trail.
//
// ── Why the CSS is injected from here ─────────────────────────────────────
//
// dashboard.html is already about a megabyte and every byte of it parses on
// every load of every view. The styles for one screen belong with that
// screen, and cygenix-assistant.js and cygenix-tour.js already set the
// precedent. Everything is namespaced under .cyg-a- so it cannot leak into
// the other forty views in that document.
//
// ── Access ────────────────────────────────────────────────────────────────
//
// The server decides. A 403 renders the "Admins only" panel, and that
// refusal has already been written to the trail as audit.view.denied by the
// time this code sees it. The sidebar hides the item for roles that cannot
// read the organisation trail, but hiding is a courtesy: this screen has to
// handle someone arriving by direct link, which is exactly what the panel
// is for.

(function (root) {
  'use strict';

  var ENDPOINT = '/.netlify/functions/audit';

  var state = {
    loaded: false,
    denied: false,
    error: null,
    status: null,
    events: [],
    total: 0,
    nextCursor: null,
    indexed: true,
    tab: 'events',
    filters: { q: '', days: 7, actor: '', action: '', target: '', outcome: '', env: '', category: '' },
    facets: { actors: [], actions: [] },
    selected: null,
    busy: false,
    verifying: false,
    verifyResult: null,
    purging: false,
    purgeResult: null,
    // The Sign-ins tab. It reads a different store from the rest of this
    // screen — the Cosmos `audit` container rather than the hash chain —
    // so it keeps its own loading state and is fetched only when the tab is
    // first opened, rather than on every visit to the audit screen.
    signins: {
      loaded: false, loading: false, error: null, denied: false,
      rows: [], scope: 'mine', days: 30, triedAll: false,
    },
  };

  var mount = null;
  var searchTimer = null;
  var modalSubmit = null;   // the open modal's submit handler; see modal()

  // ── Small helpers ───────────────────────────────────────────────────────

  function esc(s) {
    return String(s === null || s === undefined ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function token() {
    try { return (typeof root.getCygenixIdToken === 'function') ? root.getCygenixIdToken() : ''; }
    catch (e) { return ''; }
  }

  function api(qs, opts) {
    var t = token();
    if (!t) return Promise.reject(new Error('Sign in to view the audit log.'));
    var url = ENDPOINT + (qs ? '?' + qs : '');
    var init = Object.assign({ headers: {} }, opts || {});
    init.headers = Object.assign({ Authorization: 'Bearer ' + t }, init.headers);
    if (init.body) init.headers['Content-Type'] = 'application/json';
    return fetch(url, init).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (d) {
        if (r.status === 403) { var e = new Error(d.error || 'Not permitted'); e.denied = true; throw e; }
        if (!r.ok) throw new Error(d.error || ('HTTP ' + r.status));
        return d;
      });
    });
  }

  function post(body) {
    return api('', { method: 'POST', body: JSON.stringify(body) });
  }

  // Dates are shown in the reader's own locale and zone. An audit log that
  // renders everything in UTC is technically correct and practically useless
  // — "was that during the maintenance window?" is a question about local
  // time. The ISO timestamp is in the drawer for anyone who needs the
  // unambiguous form.
  function fmt(iso) {
    var d = new Date(iso);
    if (isNaN(d)) return '—';
    return d.toLocaleString(undefined, { day: '2-digit', month: 'short',
      hour: '2-digit', minute: '2-digit', second: '2-digit' }).replace(',', '');
  }

  function rel(iso) {
    var m = Math.round((Date.now() - Date.parse(iso)) / 60000);
    if (!isFinite(m)) return '';
    if (m < 1) return 'just now';
    if (m < 60) return m + ' min ago';
    if (m < 1440) return Math.round(m / 60) + ' h ago';
    return Math.round(m / 1440) + ' d ago';
  }

  function initials(name, email) {
    var n = String(name || '').trim();
    if (n) return n.split(/\s+/).map(function (x) { return x[0]; }).join('').slice(0, 2).toUpperCase();
    return String(email || '?').slice(0, 2).toUpperCase();
  }

  // A stable colour per actor, derived from the address rather than assigned,
  // so the same person is the same colour on every machine and across
  // reloads without anything being stored.

  function toast(msg) {
    var t = document.getElementById('cyg-a-toast');
    if (!t) { t = document.createElement('div'); t.id = 'cyg-a-toast'; t.className = 'cyg-a-toast'; document.body.appendChild(t); }
    t.textContent = msg;
    t.classList.add('on');
    clearTimeout(t._timer);
    t._timer = setTimeout(function () { t.classList.remove('on'); }, 2800);
  }

  // ── Styles ──────────────────────────────────────────────────────────────

  function injectStyles() {
    if (document.getElementById('cyg-audit-styles')) return;
    var s = document.createElement('style');
    s.id = 'cyg-audit-styles';
    s.textContent = [
      /* Audit log in the console design language (Phase 5, Sep-2026). Every
         colour is a token from cygenix-console.css; hue is kept for state —
         a verified chain is state-ok, a break is state-fail, a PROD event
         takes the state-fail border on its category tag — and nothing else
         on the screen carries a colour of its own. The integrity band is
         the one blueprint frame on the screen. Radius 0 throughout. */
      '.cyg-a-head{display:flex;align-items:flex-start;justify-content:space-between;gap:24px;flex-wrap:wrap;margin-bottom:18px}',
      '.cyg-a-head .cx-head-actions{padding-top:22px}',
      '.cyg-a-btn{height:32px;border:1px solid var(--color-divider);background:transparent;color:var(--color-text);padding:0 15px;cursor:pointer;display:inline-flex;align-items:center;gap:6px;font-family:var(--font-heading);font-weight:600;font-size:14px;line-height:1.2;white-space:nowrap;transition:background .12s,color .12s}',
      '.cyg-a-btn:hover:not(:disabled){background:var(--hover-tint)}',
      '.cyg-a-btn:disabled{opacity:.45;cursor:not-allowed}',
      '.cyg-a-btn.primary{background:var(--color-accent);border-color:var(--color-accent);color:#fff}',
      '.cyg-a-btn.primary:hover:not(:disabled){background:var(--color-accent-600);border-color:var(--color-accent-600)}',
      '.cyg-a-btn.danger{background:transparent;border-color:var(--state-fail);color:var(--state-fail)}',
      '.cyg-a-btn.warn{background:transparent;border-color:var(--state-warn);color:var(--state-warn)}',
      '.cyg-a-btn.sm{height:30px;padding:0 13px}',
      '.cyg-a-link{border:0;background:none;color:var(--color-accent-700);font-family:var(--font-body);font-size:14px;cursor:pointer;padding:0 4px}',
      '.cyg-a-link:hover{text-decoration:underline}',
      '.cyg-a-note-h{font-size:14px;color:var(--color-neutral-700);line-height:1.5}',
      /* the tabs: the console strip */
      '.cyg-a-tabs{display:flex;gap:0;border-bottom:1px solid var(--color-divider);margin-bottom:22px;overflow-x:auto}',
      '.cyg-a-tab{border:0;background:none;padding:8px 14px 9px;cursor:pointer;color:var(--color-neutral-600);border-bottom:2px solid transparent;margin-bottom:-1px;font-family:var(--font-heading);font-weight:600;font-size:16px;letter-spacing:.06em;text-transform:uppercase;white-space:nowrap;line-height:1.2}',
      '.cyg-a-tab:hover{color:var(--color-text)}',
      '.cyg-a-tab[aria-selected="true"]{color:var(--color-text);border-color:var(--color-accent)}',
      /* the integrity band */
      '.cyg-a-band{padding:16px 20px;display:flex;align-items:flex-start;gap:28px;flex-wrap:wrap;margin:0 0 22px}',
      '.cyg-a-band .k{font-size:13px;letter-spacing:.1em;text-transform:uppercase;color:var(--color-neutral-600)}',
      '.cyg-a-band .v{font-family:var(--font-heading);font-weight:600;font-size:22px;line-height:1.05;text-transform:uppercase;margin-top:6px;color:var(--color-text);font-variant-numeric:tabular-nums}',
      '.cyg-a-band .v.ok{color:var(--state-ok)}.cyg-a-band .v.fail{color:var(--state-fail)}.cyg-a-band .v.dim{color:var(--color-neutral-600)}',
      '.cyg-a-band .v.plain{text-transform:none;font-family:var(--font-body);font-weight:400;font-size:18px;margin-top:8px}',
      '.cyg-a-band .caveat{margin-left:auto;max-width:42ch;font-size:14px;line-height:1.5;color:var(--color-neutral-700);align-self:flex-end}',
      '.cyg-a-band .caveat.fail{color:var(--color-text);border-left:3px solid var(--state-fail);padding-left:12px}',
      /* the capture state line */
      '.cyg-a-status{display:flex;align-items:center;gap:16px;flex-wrap:wrap;padding:12px 0;border-bottom:1px solid var(--color-divider);margin-bottom:18px}',
      '.cyg-a-status h3{margin:0;font-size:15px;font-weight:400;display:flex;align-items:center;gap:10px;flex-wrap:wrap;color:var(--color-text)}',
      '.cyg-a-status p{margin:2px 0 0;color:var(--color-neutral-700);font-size:13px;line-height:1.5}',
      '.cyg-a-status .txt{flex:1;min-width:240px}',
      '.cyg-a-status.paused .cyg-a-seg button.on{color:var(--state-warn)}',
      '.cyg-a-status.off .cyg-a-seg button.on{color:var(--state-fail)}',
      '.cyg-a-pill{display:inline-flex;align-items:center;gap:6px;font-family:var(--font-heading);font-weight:600;font-size:12px;letter-spacing:.1em;text-transform:uppercase;padding:2px 8px;line-height:1.4;border:1px solid var(--color-divider);color:var(--color-neutral-700);white-space:nowrap}',
      '.cyg-a-p-green{color:var(--state-ok);border-color:var(--state-ok)}',
      '.cyg-a-p-amber{color:var(--state-warn);border-color:var(--state-warn)}',
      '.cyg-a-p-red{color:var(--state-fail);border-color:var(--state-fail)}',
      '.cyg-a-p-grey{color:var(--color-neutral-700);border-color:var(--color-divider)}',
      '.cyg-a-p-purple{color:var(--color-accent-800);border-color:var(--color-accent-300);background:var(--color-accent-100)}',
      '.cyg-a-dot{width:8px;height:8px;background:currentColor;flex:none}',
      '.cyg-a-dot.live{animation:cygADot 1.6s infinite}',
      '@keyframes cygADot{0%{box-shadow:0 0 0 0 currentColor}70%{box-shadow:0 0 0 6px transparent}100%{box-shadow:0 0 0 0 transparent}}',
      '@media (prefers-reduced-motion:reduce){.cyg-a-dot.live{animation:none}}',
      '.cyg-a-always{font-size:13px;color:var(--color-neutral-700);display:flex;gap:8px;align-items:flex-start;max-width:52ch}',
      '.cyg-a-always .ic{flex:none;margin-top:2px}',
      '.cyg-a-seg{display:inline-flex;border:1px solid var(--color-divider)}',
      '.cyg-a-seg button{border:0;background:transparent;padding:7px 16px;cursor:pointer;font-family:var(--font-heading);font-weight:600;font-size:14px;letter-spacing:.08em;text-transform:uppercase;color:var(--color-neutral-600);display:flex;align-items:center;gap:7px;line-height:1.2}',
      '.cyg-a-seg button:hover:not(:disabled){color:var(--color-text)}',
      '.cyg-a-seg button:disabled{opacity:.5;cursor:not-allowed}',
      '.cyg-a-seg button.on{background:var(--color-accent-900);color:var(--color-bg)}',
      /* the four measures */
      '.cyg-a-kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:1px;background:var(--color-divider);border:1px solid var(--color-divider);margin-bottom:22px}',
      '.cyg-a-kpi{padding:14px 18px 16px;background:var(--color-bg)}',
      '.cyg-a-kpi .l{font-size:13px;letter-spacing:.1em;text-transform:uppercase;color:var(--color-neutral-600)}',
      '.cyg-a-kpi .v{font-family:var(--font-heading);font-weight:600;font-size:32px;line-height:.95;margin-top:8px;font-variant-numeric:tabular-nums;color:var(--color-text)}',
      '.cyg-a-kpi .d{font-size:13px;color:var(--color-neutral-700);margin-top:6px}',
      /* events: the filter row and the table */
      '.cyg-a-toolbar{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:12px}',
      '.cyg-a-in,.cyg-a-sel{height:34px;border:1px solid var(--color-divider);background:var(--color-bg);color:var(--color-text);padding:0 10px;font-family:var(--font-body);font-size:14px}',
      '.cyg-a-in{min-width:220px;flex:0 1 260px}',
      '.cyg-a-count{margin-left:auto;font-size:14px;color:var(--color-neutral-700);font-variant-numeric:tabular-nums}',
      /* Sign-ins tab: the standing note under the table, and the two-letter
         country beside a place. The country code is a quiet confirmation of
         the place name, not a second copy of it, so it is small and grey. */
      '.cyg-a-note{font-size:13px;color:var(--color-neutral-700);line-height:1.6;margin:12px 0 0;max-width:70ch}',
      '.cyg-a-note.err{color:var(--state-fail)}',
      '.cyg-a-cc{margin-left:7px;font-size:11px;letter-spacing:.06em;color:var(--color-neutral-600);' +
        'border:1px solid var(--color-divider);padding:1px 5px;vertical-align:1px}',
      '.cyg-a-sp{flex:1}',
      '.cyg-a-chips{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px}',
      '.cyg-a-chip{border:1px solid var(--color-divider);background:transparent;color:var(--color-neutral-700);padding:4px 10px;font-family:var(--font-body);font-size:13px;cursor:pointer;line-height:1.2}',
      '.cyg-a-chip:hover{color:var(--color-text)}',
      '.cyg-a-chip[aria-pressed="true"]{background:var(--color-accent-100);border-color:var(--color-accent);color:var(--color-accent-800)}',
      '.cyg-a-card{border:1px solid var(--color-divider);background:transparent}',
      '.cyg-a-tw{overflow-x:auto}',
      '.cyg-a-table{width:100%;border-collapse:collapse;font-size:13px;line-height:1.4}',
      '.cyg-a-table th{text-align:left;font-family:var(--font-body);font-weight:400;font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:color-mix(in srgb,var(--color-text) 60%,transparent);background:transparent;padding:8px 10px;border-bottom:1px solid var(--color-divider);white-space:nowrap}',
      '.cyg-a-table th.r,.cyg-a-table td.r{text-align:right}',
      '.cyg-a-table td{padding:8px 10px;border-bottom:1px solid var(--color-divider);vertical-align:top;color:var(--color-text)}',
      /* The filter row. Sticky is deliberate: the whole point of putting a
         filter in a column header is that you can see which column it
         governs, and that stops being true the moment it scrolls away. */
      '.cyg-a-table tr.cyg-a-filters th{background:var(--color-bg);padding:6px 6px;border-bottom:1px solid var(--color-divider);position:sticky;top:0;z-index:1}',
      '.cyg-a-fsel,.cyg-a-fin{width:100%;min-width:96px;max-width:230px;height:30px;font-family:var(--font-body);font-size:13px;' +
        'border:1px solid var(--color-divider);background:var(--color-bg);color:var(--color-text);padding:0 7px}',
      '.cyg-a-fsel:focus-visible,.cyg-a-fin:focus-visible{outline:2px solid var(--color-accent);outline-offset:1px}',
      /* A set filter is tinted, so a table that looks empty because of one is
         visibly different from a table that is empty. */
      '.cyg-a-fsel.on,.cyg-a-fin.on{border-color:var(--color-accent);background:var(--color-accent-100);color:var(--color-accent-800)}',
      '.cyg-a-fsel:disabled{opacity:.5;cursor:not-allowed}',
      '.cyg-a-table tr.ev{cursor:pointer}',
      '.cyg-a-table tr.ev:hover td{background:var(--hover-tint)}',
      '.cyg-a-table tr.ev.sel td{background:var(--color-accent-100)}',
      '.cyg-a-table tr.ev:focus-visible{outline:2px solid var(--color-accent);outline-offset:-2px}',
      '.cyg-a-seq{font-variant-numeric:tabular-nums;color:var(--color-neutral-700);white-space:nowrap}',
      '.cyg-a-when{white-space:nowrap;font-variant-numeric:tabular-nums}',
      '.cyg-a-when small{display:block;color:var(--color-neutral-600);font-size:13px}',
      '.cyg-a-who{min-width:160px;word-break:break-all}',
      '.cyg-a-who small{display:block;color:var(--color-neutral-600);font-size:13px}',
      '.cyg-a-cat{font-family:var(--font-heading);font-weight:600;font-size:12px;letter-spacing:.1em;text-transform:uppercase;padding:2px 8px;line-height:1.4;border:1px solid var(--color-divider);color:var(--color-neutral-700);white-space:nowrap;display:inline-block}',
      '.cyg-a-cat.prod{color:var(--state-fail);border-color:var(--state-fail)}',
      '.cyg-a-act{font-family:var(--mono);font-size:12px;color:var(--color-text);white-space:nowrap}',
      '.cyg-a-what .s{color:var(--color-neutral-700);font-size:13px;margin-top:2px}',
      '.cyg-a-tgt{min-width:140px}',
      '.cyg-a-env{font-size:13px;color:var(--color-neutral-700)}',
      '.cyg-a-env.PROD{color:var(--state-fail)}',
      '.cyg-a-out{font-size:13px}.cyg-a-out.allowed{color:var(--state-ok)}.cyg-a-out.denied{color:var(--state-fail)}.cyg-a-out.failed{color:var(--state-warn)}',
      '.cyg-a-srcw{font-size:13px;color:var(--color-neutral-600);white-space:nowrap}',
      '.cyg-a-ai{font-size:12px;color:var(--color-accent-800);border:1px solid var(--color-accent-300);background:var(--color-accent-100);padding:0 5px;margin-left:6px;white-space:nowrap}',
      '.cyg-a-src{font-size:12px;color:var(--color-neutral-600);border:1px solid var(--color-divider);padding:0 4px;margin-left:6px}',
      '.cyg-a-table tr.gap td{background:transparent;color:var(--color-neutral-800);font-size:13px;padding:8px 12px;border-left:3px solid var(--state-warn)}',
      '.cyg-a-table tr.gap.off td{border-left-color:var(--state-fail)}',
      '.cyg-a-foot{display:flex;justify-content:space-between;align-items:center;padding:10px 12px;color:var(--color-neutral-700);font-size:13px;flex-wrap:wrap;gap:8px;border-top:1px solid var(--color-divider)}',
      '.cyg-a-empty{padding:40px;text-align:center;color:var(--color-neutral-700);font-size:14px}',
      /* the drawer */
      '.cyg-a-scrim{position:fixed;inset:0;background:var(--modal-scrim);opacity:0;pointer-events:none;transition:opacity .2s;z-index:1400}',
      '.cyg-a-scrim.on{opacity:1;pointer-events:auto}',
      '.cyg-a-drawer{position:fixed;top:0;right:0;height:100vh;width:min(520px,100vw);background:var(--color-bg);border-left:1px solid var(--color-divider);box-shadow:var(--shadow-strong);transform:translateX(105%);transition:transform .22s ease;display:flex;flex-direction:column;z-index:1401}',
      '.cyg-a-drawer.on{transform:none}',
      '@media (prefers-reduced-motion:reduce){.cyg-a-drawer{transition:none}}',
      '.cyg-a-dh{padding:18px 20px;border-bottom:1px solid var(--color-divider);display:flex;gap:12px;align-items:flex-start}',
      '.cyg-a-dh h2{margin:0;font-size:22px;font-family:var(--font-heading);font-weight:600;text-transform:uppercase;line-height:1.05;word-break:break-all}',
      '.cyg-a-dh p{margin:6px 0 0;color:var(--color-neutral-700);font-size:14px}',
      '.cyg-a-x{margin-left:auto;border:0;background:none;font-size:20px;cursor:pointer;color:var(--color-neutral-700);line-height:1;padding:0 4px}',
      '.cyg-a-db{padding:16px 20px;overflow:auto;flex:1}',
      '.cyg-a-kv{display:grid;grid-template-columns:130px 1fr;gap:6px 12px;font-size:14px;margin-bottom:18px}',
      '.cyg-a-kv dt{color:var(--color-neutral-700)}.cyg-a-kv dd{margin:0;word-break:break-word}',
      '.cyg-a-db h4{font-family:var(--font-heading);font-weight:600;font-size:13px;letter-spacing:.14em;text-transform:uppercase;color:var(--color-neutral-700);margin:0 0 8px}',
      '.cyg-a-diff{border:1px solid var(--color-divider);overflow:hidden;margin-bottom:18px;font-family:var(--mono);font-size:12px}',
      '.cyg-a-diff .row{display:grid;grid-template-columns:130px 1fr 1fr;border-bottom:1px solid var(--color-divider)}',
      '.cyg-a-diff .row:last-child{border:0}',
      '.cyg-a-diff .row>div{padding:7px 10px;word-break:break-word}',
      '.cyg-a-diff .hd{background:var(--color-neutral-100);font-family:var(--font-body);font-size:11px;color:var(--color-neutral-700);letter-spacing:.08em}',
      '.cyg-a-diff .b{color:var(--state-fail)}',
      '.cyg-a-diff .a{color:var(--state-ok)}',
      '.cyg-a-hash{font-family:var(--mono);font-size:12px;background:var(--color-neutral-100);border:1px solid var(--color-divider);padding:10px;color:var(--color-neutral-700);word-break:break-all;line-height:1.7}',
      '.cyg-a-hash b{color:var(--color-text);font-weight:500}',
      /* modals */
      '.cyg-a-modal{position:fixed;inset:0;display:none;place-items:center;background:var(--modal-scrim);z-index:1500;padding:16px}',
      '.cyg-a-modal.on{display:grid}',
      '.cyg-a-mbox{background:var(--color-bg);border:1px solid var(--color-divider);width:min(460px,100%);padding:20px;box-shadow:var(--shadow-strong)}',
      '.cyg-a-mbox h3{margin:0 0 6px;font-family:var(--font-heading);font-weight:600;font-size:22px;text-transform:uppercase;line-height:1.05}',
      '.cyg-a-mbox p{color:var(--color-neutral-800);margin:0 0 14px;font-size:14px;line-height:1.5}',
      '.cyg-a-mbox label{display:block;font-size:13px;color:var(--color-neutral-700);margin:10px 0 5px}',
      '.cyg-a-mbox textarea,.cyg-a-mbox select,.cyg-a-mbox input[type=text]{width:100%;border:1px solid var(--color-divider);padding:8px 10px;background:var(--color-bg);color:var(--color-text);font-family:var(--font-body);font-size:14px}',
      '.cyg-a-mbox textarea{min-height:70px;resize:vertical}',
      '.cyg-a-mact{display:flex;justify-content:flex-end;gap:8px;margin-top:16px}',
      '.cyg-a-note{font-size:13px;line-height:1.5;border-left:3px solid var(--color-neutral-400);padding:2px 0 2px 12px;color:var(--color-neutral-800);margin-top:12px}',
      '.cyg-a-note.warn{border-left-color:var(--state-warn)}',
      /* settings, integrity, retention */
      '.cyg-a-grid2{display:grid;grid-template-columns:1fr 1fr;gap:26px;align-items:start}',
      '.cyg-a-pad{padding:18px 20px;margin-bottom:26px}',
      '.cyg-a-pad h3{margin:0 0 4px;font-family:var(--font-heading);font-weight:600;font-size:17px;text-transform:uppercase;line-height:1.2}',
      '.cyg-a-pad>p{margin:0 0 14px;color:var(--color-neutral-800);font-size:14px;line-height:1.5}',
      '.cyg-a-cat-row{display:flex;align-items:center;gap:12px;padding:10px 0;border-top:1px solid var(--color-divider)}',
      '.cyg-a-cat-row:first-of-type{border-top:0}',
      '.cyg-a-cat-row .t{flex:1}.cyg-a-cat-row .t b{font-weight:500;display:block;font-size:14px}',
      '.cyg-a-cat-row .t small{color:var(--color-neutral-700);font-size:13px;line-height:1.4}',
      /* A locked category is a fact, not a control: a word and a note, never
         a disabled switch. A disabled switch invites a click that can never
         work. */
      '.cyg-a-locked{font-family:var(--font-heading);font-weight:600;font-size:12px;letter-spacing:.1em;text-transform:uppercase;color:var(--color-neutral-700);border:1px solid var(--color-divider);padding:2px 8px;white-space:nowrap;flex:none}',
      '.cyg-a-tg{position:relative;width:34px;height:18px;flex:none;display:inline-block}',
      '.cyg-a-tg input{position:absolute;opacity:0;width:34px;height:18px;margin:0;cursor:pointer}',
      '.cyg-a-tg span{position:absolute;inset:0;background:var(--color-neutral-200);border:1px solid var(--color-divider);transition:background .15s;pointer-events:none}',
      '.cyg-a-tg span:before{content:"";position:absolute;width:12px;height:12px;background:var(--color-neutral-600);left:2px;top:2px;transition:transform .15s}',
      '.cyg-a-tg input:checked+span{background:var(--color-accent);border-color:var(--color-accent)}',
      '.cyg-a-tg input:checked+span:before{transform:translateX(16px);background:#fff}',
      '.cyg-a-tg input:disabled{cursor:not-allowed}',
      '.cyg-a-tg input:disabled+span{opacity:.55}',
      '.cyg-a-tg input:focus-visible+span{outline:2px solid var(--color-accent);outline-offset:2px}',
      '.cyg-a-radio{display:flex;gap:8px;flex-wrap:wrap}',
      '.cyg-a-radio label{border:1px solid var(--color-divider);padding:7px 12px;cursor:pointer;font-size:14px}',
      '.cyg-a-radio input{margin-right:6px}',
      '.cyg-a-chain{display:flex;gap:6px;align-items:center;overflow-x:auto;padding:6px 0 14px}',
      '.cyg-a-blk{flex:none;border:1px solid var(--color-divider);padding:8px 10px;font-family:var(--mono);font-size:12px;background:transparent;min-width:120px}',
      '.cyg-a-blk b{display:block;color:var(--color-text);font-size:12px;font-family:var(--font-body);font-weight:500}',
      '.cyg-a-arrow{color:var(--color-neutral-600);flex:none}',
      '.cyg-a-denied{max-width:520px;margin:60px auto;text-align:center;padding:36px}',
      '.cyg-a-denied .ic-wrap{width:52px;height:52px;border:1px solid var(--state-fail);color:var(--state-fail);display:grid;place-items:center;margin:0 auto 14px}',
      '.cyg-a-toast{position:fixed;bottom:22px;left:50%;transform:translateX(-50%) translateY(20px);background:var(--color-accent-900);color:var(--color-bg);padding:10px 16px;font-size:14px;opacity:0;transition:.2s;z-index:1600;pointer-events:none;max-width:90vw;text-align:center}',
      '.cyg-a-toast.on{opacity:1;transform:translateX(-50%)}',
      '.cyg-a-banner{border-left:3px solid var(--state-warn);padding:2px 0 2px 12px;color:var(--color-text);font-size:14px;line-height:1.5;margin-bottom:14px}',
      '@media (max-width:1000px){.cyg-a-kpis{grid-template-columns:repeat(2,minmax(0,1fr))}.cyg-a-grid2{grid-template-columns:1fr}.cyg-a-band .caveat{margin-left:0;max-width:none}}',
      '@media (max-width:640px){.cyg-a-in{max-width:none;width:100%}.cyg-a-kv{grid-template-columns:1fr}.cyg-a-diff .row{grid-template-columns:90px 1fr 1fr}}',
    ].join('\n');
    document.head.appendChild(s);
  }

  // ── Loading ─────────────────────────────────────────────────────────────

  function filterQuery(extra) {
    var f = state.filters;
    var p = [];
    if (f.days) {
      p.push('from=' + encodeURIComponent(new Date(Date.now() - f.days * 86400000).toISOString()));
    }
    if (f.q) p.push('q=' + encodeURIComponent(f.q));
    if (f.actor) p.push('actor=' + encodeURIComponent(f.actor));
    if (f.action) p.push('action=' + encodeURIComponent(f.action));
    if (f.target) p.push('target=' + encodeURIComponent(f.target));
    if (f.outcome) p.push('outcome=' + encodeURIComponent(f.outcome));
    if (f.env) p.push('env=' + encodeURIComponent(f.env));
    if (f.category) p.push('category=' + encodeURIComponent(f.category));
    if (extra) p.push(extra);
    return p.join('&');
  }

  function load() {
    state.busy = true;
    return Promise.all([
      api('what=status'),
      api('what=events&limit=50&' + filterQuery()),
    ]).then(function (r) {
      state.status = r[0];
      state.events = r[1].entries || [];
      state.total = r[1].total || 0;
      state.nextCursor = r[1].nextCursor || null;
      state.indexed = r[1].indexed !== false;
      state.facets = r[1].facets || { actors: [], actions: [] };
      state.loaded = true;
      state.denied = false;
      state.error = null;
    }).catch(function (e) {
      state.loaded = true;
      state.denied = !!e.denied;
      state.error = e.message;
    }).then(function () {
      state.busy = false;
      render();
    });
  }

  function reloadEvents() {
    state.busy = true;
    renderEventsPanel();
    return api('what=events&limit=50&' + filterQuery()).then(function (d) {
      state.events = d.entries || [];
      state.total = d.total || 0;
      state.nextCursor = d.nextCursor || null;
      state.indexed = d.indexed !== false;
      // Facets are computed over the date window with the column filters
      // deliberately NOT applied, so picking a person does not empty the
      // action list of every action that person did not perform.
      state.facets = d.facets || state.facets;
      state.error = null;
    }).catch(function (e) {
      state.error = e.message;
    }).then(function () {
      state.busy = false;
      renderEventsPanel();
    });
  }

  function loadMore() {
    if (!state.nextCursor) return;
    state.busy = true;
    renderEventsPanel();
    api('what=events&limit=50&cursor=' + state.nextCursor + '&' + filterQuery()).then(function (d) {
      state.events = state.events.concat(d.entries || []);
      state.nextCursor = d.nextCursor || null;
    }).catch(function (e) { state.error = e.message; })
      .then(function () { state.busy = false; renderEventsPanel(); });
  }

  // ── Rendering ───────────────────────────────────────────────────────────

  function render() {
    if (!mount) return;
    if (!state.loaded) { mount.innerHTML = '<div class="cyg-a-empty">Loading the audit trail…</div>'; return; }
    if (state.denied) { mount.innerHTML = deniedHtml(); return; }
    if (!state.status) {
      mount.innerHTML = '<div class="empty-state"><h3>Audit trail unavailable</h3><p>' +
        esc(state.error || 'Could not reach the audit service.') + '</p></div>';
      return;
    }
    mount.innerHTML =
      headerHtml() + tabsHtml() + bandHtml() + bannerHtml() + statusHtml() + kpiHtml() +
      '<div id="cyg-a-panel-events" role="tabpanel" aria-labelledby="cyg-a-tab-events"' +
        (state.tab === 'events' ? '' : ' hidden') + '></div>' +
      '<div id="cyg-a-panel-signins" role="tabpanel" aria-labelledby="cyg-a-tab-signins"' +
        (state.tab === 'signins' ? '' : ' hidden') + '></div>' +
      '<div id="cyg-a-panel-settings" role="tabpanel" aria-labelledby="cyg-a-tab-settings"' +
        (state.tab === 'settings' ? '' : ' hidden') + '></div>' +
      '<div id="cyg-a-panel-integrity" role="tabpanel" aria-labelledby="cyg-a-tab-integrity"' +
        (state.tab === 'integrity' ? '' : ' hidden') + '></div>' +
      '<div id="cyg-a-panel-retention" role="tabpanel" aria-labelledby="cyg-a-tab-retention"' +
        (state.tab === 'retention' ? '' : ' hidden') + '></div>';
    renderEventsPanel();
    renderSigninsPanel();
    renderSettingsPanel();
    renderIntegrityPanel();
    renderRetentionPanel();
    wire();
  }

  // ── Header and the integrity band ───────────────────────────────────────
  //
  // Kicker, title, and the two actions the whole screen exists for: get the
  // evidence out, and prove the chain. Verify is the filled one because it
  // is the one an auditor presses first.
  function headerHtml() {
    var st = state.status;
    return '<div class="cx-head cyg-a-head"><div>' +
      '<div class="cx-kicker">Govern · Hash-chained, append only</div>' +
      '<h1 class="cx-title">Audit log</h1></div>' +
      '<div class="cx-head-actions">' +
      (st.canExport
        ? '<button class="cyg-a-btn" id="cyg-a-csv" type="button" title="Every matching entry as CSV, with its hashes — the export is itself written to the trail">Export evidence pack</button>' +
          '<button class="cyg-a-link" id="cyg-a-json" type="button" title="The same rows as JSON">JSON</button>'
        : '<span class="cyg-a-note-h">Export needs the Platform Administrator or Auditor role</span>') +
      (st.canVerify
        ? '<button class="cyg-a-btn primary" id="cyg-a-verify" type="button"' +
          (state.verifying ? ' disabled' : '') + '>' + (state.verifying ? 'Verifying…' : 'Verify chain') + '</button>'
        : '') +
      '</div></div>';
  }

  // What the chain is verified to, stated as a sentence in the status colour.
  // A verification run in this session outranks the one the trail recorded;
  // neither is invented — with no verification on record the band says so.
  function verifyFact() {
    var v = state.verifyResult || state.status.lastVerify || null;
    if (!v) return { kind: 'none' };
    var at = v.verifiedAt || v.at || null;
    if (v.ok === false) return { kind: 'break', at: at, brokenAt: v.brokenAt, reason: v.reason };
    // The head at the time of the check is what was verified; a verify in
    // this session covers the head as loaded, a recorded one the entry that
    // recorded it (the verify entry follows the entries it checked).
    var to = state.verifyResult ? (state.status.headSeq || v.count || 0) : (v.seq ? v.seq - 1 : (v.count || 0));
    return { kind: 'ok', at: at, to: to, count: v.count };
  }

  function bandHtml() {
    var st = state.status;
    var f = verifyFact();
    var cats = (st.categories || []).length;
    var locked = (st.alwaysOn || []).length;
    var chain = f.kind === 'ok'
      ? '<div class="v ok">Verified to entry ' + esc(Number(f.to || 0).toLocaleString('en-GB')) + '</div>'
      : f.kind === 'break'
        ? '<div class="v fail">Break at entry ' + esc(Number(f.brokenAt || 0).toLocaleString('en-GB')) + '</div>'
        : '<div class="v dim">Not yet verified</div>';
    var last = f.at ? (rel(f.at) === 'just now' ? 'Just now' : esc(fmt(f.at))) : '—';
    var caveat = f.kind === 'break'
      ? '<div class="caveat fail"><b>Entry ' + esc(f.brokenAt) + ' does not line up' +
        (f.reason ? ' — ' + esc(f.reason) : '') + '.</b> Export the trail before anything else is written, ' +
        'compare that entry against the archive and the retention checkpoint, and record the finding as an incident.</div>'
      : '<div class="caveat">A simultaneous append can race the head. A break is reported, never hidden — state that in any evidence pack.</div>';
    return '<div class="cx-blueprint cyg-a-band" id="cyg-a-band">' +
      '<span class="cx-corner tl"></span><span class="cx-corner tr"></span><span class="cx-corner bl"></span><span class="cx-corner br"></span>' +
      '<div><div class="k">Chain</div>' + chain + '</div>' +
      '<div><div class="k">Last verified</div><div class="v plain">' + last + '</div></div>' +
      '<div><div class="k">Capture</div><div class="v plain">' + cats + ' categor' + (cats === 1 ? 'y' : 'ies') +
        ' · ' + locked + ' locked</div></div>' +
      caveat + '</div>';
  }

  function deniedHtml() {
    return '<div class="cyg-a-card cyg-a-denied">' +
      '<div class="ic-wrap"><i class="ic ic-lock ic-lg"></i></div>' +
      '<h3 style="margin:0 0 6px;font-size:18px">You cannot read the organisation audit trail</h3>' +
      '<p style="color:var(--text2);margin:0 0 16px">The full trail is readable by the Organisation Owner, ' +
      'the Platform Administrator and the Auditor. Delivery roles can see their own entries. ' +
      'Ask an administrator if you need wider access.</p>' +
      '<span class="cyg-a-pill cyg-a-p-grey">This attempt was recorded as ' +
      '<span style="font-family:var(--mono)">audit.view.denied</span></span></div>';
  }

  // An overdue pause means capture stopped and the resume never fired. It is
  // the one state worth shouting about, because everything looks normal from
  // the outside while nothing is being recorded. Kept out of statusHtml() so
  // the card can be re-rendered on its own without the banner coming with it.
  function bannerHtml() {
    var overdue = (state.status.gaps || []).filter(function (g) { return g.overdue; });
    if (!overdue.length) return '';
    return '<div class="cyg-a-banner"><b>A pause outlived its end time.</b> Capture was paused at ' +
      esc(fmt(overdue[0].from)) + ' and should have resumed at ' + esc(fmt(overdue[0].expectedTo)) +
      ', but no resume was recorded. Reading this page resolves it — reload to confirm capture is back on.</div>';
  }

  function statusHtml() {
    var st = state.status;
    var s = st.state;
    var pill = s === 'recording'
      ? '<span class="cyg-a-pill cyg-a-p-green"><span class="cyg-a-dot live"></span>Recording</span>'
      : s === 'paused'
        ? '<span class="cyg-a-pill cyg-a-p-amber"><i class="ic ic-pause ic-sm"></i>Paused</span>'
        : '<span class="cyg-a-pill cyg-a-p-red"><i class="ic ic-stop ic-sm"></i>Off</span>';

    var on = 0, total = (st.categories || []).length;
    (st.categories || []).forEach(function (c) {
      if (st.settings.categories[c.key] !== false) on++;
    });

    var title, text;
    if (s === 'recording') {
      title = 'Capturing all enabled categories';
      text = on + ' of ' + total + ' categories on · ' +
        (st.settings.retentionDays === 2555 ? '7 years' : st.settings.retentionDays === 90 ? '90 days' : '1 year') +
        ' retention';
    } else if (s === 'paused') {
      var until = st.pausedUntil ? new Date(st.pausedUntil) : null;
      title = until ? 'Resumes by itself at ' +
        until.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' }) : 'Paused';
      text = 'Paused by ' + esc(st.changedBy || 'an administrator') +
        (st.reason ? ' — "' + esc(st.reason) + '"' : '') +
        '. General changes are not being captured.';
    } else {
      title = 'General change capture is off';
      text = 'Turned off by ' + esc(st.changedBy || 'an administrator') +
        (st.reason ? ' — "' + esc(st.reason) + '"' : '') +
        '. Off has no timer — it stays off until an administrator turns it back on.';
    }

    var canCfg = st.canConfigure;
    var seg = '<div class="cyg-a-seg" id="cyg-a-state-seg" role="group" aria-label="Audit capture state">' +
      '<button type="button" data-s="recording"' + (s === 'recording' ? ' class="on"' : '') +
        (canCfg ? '' : ' disabled') + ' aria-pressed="' + (s === 'recording') + '">' +
        '<span class="cyg-a-dot"></span>Recording</button>' +
      '<button type="button" data-s="paused"' + (s === 'paused' ? ' class="on"' : '') +
        (canCfg ? '' : ' disabled') + ' aria-pressed="' + (s === 'paused') + '">' +
        '<i class="ic ic-pause ic-sm"></i>Pause</button>' +
      '<button type="button" data-s="off"' + (s === 'off' ? ' class="on"' : '') +
        (canCfg ? '' : ' disabled') + ' aria-pressed="' + (s === 'off') + '">' +
        '<i class="ic ic-stop ic-sm"></i>Off</button></div>';

    return '<div class="cyg-a-status' + (s === 'recording' ? '' : ' ' + s) + '">' +
      '<div class="txt"><h3>' + pill + '<span>' + esc(title) + '</span></h3><p>' + text + '</p></div>' +
      '<div class="cyg-a-always"><i class="ic ic-shield ic-sm"></i><span>Security events are ' +
      '<b>always recorded</b>, whatever the state: sign-ins, users and roles, PROD writes, ' +
      'and every change to this switch itself.</span></div>' +
      seg + '</div>';
  }

  function kpiHtml() {
    var s = state.status.stats;
    // Self-scoped readers get no organisation KPIs, because a count of
    // "people making changes" computed over one person's own slice would be
    // a true number that answers a different question.
    if (!s) {
      return '<div class="cyg-a-note" style="margin:0 0 22px">You are seeing your own entries. ' +
        'Organisation-wide counts need the Owner, Platform Administrator or Auditor role.</div>';
    }
    var tile = function (label, value, detail, colour) {
      return '<div class="cyg-a-kpi"><div class="l">' + esc(label) + '</div>' +
        '<div class="v"' + (colour ? ' style="color:' + colour + '"' : '') + '>' + value + '</div>' +
        '<div class="d">' + esc(detail) + '</div></div>';
    };
    return '<div class="cyg-a-kpis">' +
      tile('Events · last 24h', s.events, 'across all projects') +
      tile('People making changes', s.actors, 'distinct actors, 24h') +
      tile('PROD changes', s.prodChanges, 'writes and config on PROD', s.prodChanges ? 'var(--state-fail)' : '') +
      tile('Denied / failed', s.deniedOrFailed, 'blocked by role, or errored',
           s.deniedOrFailed ? 'var(--state-warn)' : '') +
      '</div>';
  }

  var TABS = [
    { key: 'events', label: 'Events' },
    { key: 'signins', label: 'Sign-ins' },
    { key: 'settings', label: 'Capture settings' },
    { key: 'integrity', label: 'Integrity' },
    { key: 'retention', label: 'Retention' },
  ];

  function tabsHtml() {
    // A conformant tab widget: roving tabindex, aria-selected, arrow keys.
    // The console's older tab bars are div soup; new ones are not.
    return '<div class="cyg-a-tabs" role="tablist" aria-label="Audit log sections">' +
      TABS.map(function (t) {
        var on = state.tab === t.key;
        return '<button type="button" class="cyg-a-tab" role="tab" id="cyg-a-tab-' + t.key + '" ' +
          'data-tab="' + t.key + '" aria-selected="' + on + '" ' +
          'aria-controls="cyg-a-panel-' + t.key + '" tabindex="' + (on ? '0' : '-1') + '">' +
          esc(t.label) + '</button>';
      }).join('') + '</div>';
  }

  // ── Events tab ──────────────────────────────────────────────────────────

  // Which text box had focus, and where the caret was. The panel is rebuilt
  // from scratch on every reload, and a debounced search that rebuilds the
  // input the user is typing into takes their focus and their caret with it —
  // so the second word goes nowhere. Captured before the rebuild, restored
  // after. (page.fill() in a browser test sets a value in one go and never
  // notices this, which is why it survived the first round.)
  function captureFocus() {
    var a = document.activeElement;
    if (!a || (a.tagName !== 'INPUT' && a.tagName !== 'SELECT')) return null;
    var key = a.id || (a.dataset && a.dataset.filter ? 'filter:' + a.dataset.filter : '');
    if (!key) return null;
    return { key: key, start: a.selectionStart, end: a.selectionEnd };
  }

  function restoreFocus(saved) {
    if (!saved) return;
    var panel = document.getElementById('cyg-a-panel-events');
    if (!panel) return;
    var node = saved.key.indexOf('filter:') === 0
      ? panel.querySelector('[data-filter="' + saved.key.slice(7) + '"]')
      : document.getElementById(saved.key);
    if (!node) return;
    node.focus();
    try {
      if (saved.start != null && node.setSelectionRange) node.setSelectionRange(saved.start, saved.end);
    } catch (e) { /* selectionRange throws on input types that have no caret */ }
  }

  function renderEventsPanel() {
    var el = document.getElementById('cyg-a-panel-events');
    if (!el) return;
    var focused = captureFocus();
    var f = state.filters;
    var st = state.status;

    // The toolbar keeps only the two things that are not about one column:
    // free text across everything, and the exports. Everything else moved
    // into the column it filters — with six columns and rows this dense, a
    // separate strip of unlabelled dropdowns makes the reader work out which
    // control governs which column before they can use either.
    // The filter row: free text across everything, the category chips, a
    // Clear control while anything is set, and the count pushed right. The
    // exports moved to the header; the per-column filters stay in the column
    // they govern, below.
    var windowWord = f.days === 1 ? 'last 24 hours' : f.days === 7 ? 'last 7 days' : f.days === 30 ? 'last 30 days' : 'all time';
    var toolbar = '<div class="cyg-a-toolbar">' +
      '<input class="cyg-a-in" id="cyg-a-q" type="search" placeholder="Filter by actor, object or id" ' +
        'aria-label="Search all columns" value="' + esc(f.q) + '">' +
      (activeFilterCount()
        ? '<button class="cyg-a-btn sm" id="cyg-a-clear" type="button">Clear ' +
          activeFilterCount() + ' filter' + (activeFilterCount() === 1 ? '' : 's') + '</button>'
        : '') +
      '<span class="cyg-a-count" id="cyg-a-count">' + Number(state.total || 0).toLocaleString('en-GB') +
        ' event' + (state.total === 1 ? '' : 's') + ' · ' + windowWord + '</span>' +
      '</div>';

    var chips = '<div class="cyg-a-chips" id="cyg-a-chips">' +
      [{ key: '', label: 'All' }].concat(st.categories || []).map(function (c) {
        return '<button type="button" class="cyg-a-chip" data-cat="' + esc(c.key) + '" ' +
          'aria-pressed="' + (f.category === c.key) + '">' + esc(c.label) +
          (c.alwaysOn ? ' <i class="ic ic-lock ic-sm"></i>' : '') + '</button>';
      }).join('') + '</div>';

    var rows = buildRows();
    var unindexed = state.indexed ? '' :
      '<div class="cyg-a-banner">These results were read by walking the chain rather than the search ' +
      'index, because no index covers this window — entries written before indexing was added have none. ' +
      'The oldest 1,000 entries are searched; older ones are in the export.</div>';

    el.innerHTML = toolbar + chips + unindexed +
      '<div class="cyg-a-card"><div class="cyg-a-tw"><table class="cyg-a-table">' +
      '<thead><tr><th>Seq</th><th>When</th><th>Actor</th><th>Category</th><th>Action</th><th>Object</th>' +
      '<th>Env</th><th>Outcome</th><th class="r">Source</th></tr>' +
      filterRow(f) + '</thead>' +
      '<tbody id="cyg-a-rows">' + rows + '</tbody></table></div>' +
      '<div class="cyg-a-foot"><span>' + state.events.length + ' shown of ' + state.total +
      ' matching · ' + (state.status.scope === 'self' ? 'your own entries' : 'organisation-wide') + '</span>' +
      '<span>' + (state.nextCursor
        ? '<button class="cyg-a-btn" id="cyg-a-more" type="button"' + (state.busy ? ' disabled' : '') + '>' +
          (state.busy ? 'Loading…' : 'Load 50 more') + '</button>'
        : 'Click a row for the before/after diff and the chain hash') +
      '</span></div></div>';

    wireEvents();
    restoreFocus(focused);
  }

  // How many column filters are set. Drives the Clear button, which exists
  // because a filtered table that looks empty and a genuinely empty table
  // look identical, and the second is a much worse thing to conclude about
  // an audit log.
  function activeFilterCount() {
    var f = state.filters;
    return (f.days !== 7 ? 1 : 0) + (f.actor ? 1 : 0) + (f.action ? 1 : 0) +
           (f.target ? 1 : 0) + (f.env ? 1 : 0) + (f.outcome ? 1 : 0) +
           (f.category ? 1 : 0) + (f.q ? 1 : 0);
  }

  function optionList(values, current, anyLabel) {
    return [{ value: '', label: anyLabel }].concat(values).map(function (o) {
      var v = o.value === undefined ? o : o.value;
      var l = o.label === undefined ? v : o.label;
      return '<option value="' + esc(v) + '"' + (String(current || '') === String(v) ? ' selected' : '') +
        '>' + esc(l) + '</option>';
    }).join('');
  }

  // One control per column, in the column. Selects where the set of values is
  // known and bounded, a text box where it is not.
  //
  // The person and action lists come from the SERVER's facets, computed over
  // the whole date window rather than over the fifty rows on screen — a
  // dropdown built from the current page silently omits the person somebody
  // is looking for, which on this screen is the difference between "they did
  // nothing" and "you cannot see it from here".
  function filterRow(f) {
    var facets = state.facets || { actors: [], actions: [] };
    var cell = function (inner) { return '<th class="cyg-a-fcell">' + inner + '</th>'; };
    // `on` marks a filter that is actually set, so a table that looks empty
    // because of one is visibly different from a table that is empty.
    var cls = function (base, set) { return base + (set ? ' on' : ''); };
    return '<tr class="cyg-a-filters">' +
      cell('') +
      cell('<select class="' + cls('cyg-a-fsel', f.days !== 7) + '" data-filter="days" aria-label="Filter by date range">' +
        [[1, 'Last 24 hours'], [7, 'Last 7 days'], [30, 'Last 30 days'], [0, 'All time']]
          .map(function (o) {
            return '<option value="' + o[0] + '"' + (f.days === o[0] ? ' selected' : '') + '>' +
              o[1] + '</option>';
          }).join('') + '</select>') +
      cell('<select class="' + cls('cyg-a-fsel', f.actor) + '" data-filter="actor" aria-label="Filter by person"' +
        (state.status.scope === 'self' ? ' disabled title="You are seeing your own entries"' : '') + '>' +
        optionList(facets.actors.map(function (a) {
          return { value: a.value, label: a.value + ' (' + a.count + ')' };
        }), f.actor, 'Anyone') + '</select>') +
      cell('') +
      cell('<select class="' + cls('cyg-a-fsel', f.action) + '" data-filter="action" aria-label="Filter by action">' +
        optionList(facets.actions.map(function (a) {
          return { value: a.value, label: a.value + ' (' + a.count + ')' };
        }), f.action, 'Any action') + '</select>') +
      cell('<input class="' + cls('cyg-a-fin', f.target) + '" data-filter="target" type="search" ' +
        'placeholder="Any target" aria-label="Filter by target" value="' + esc(f.target) + '">') +
      cell('<select class="' + cls('cyg-a-fsel', f.env) + '" data-filter="env" aria-label="Filter by environment">' +
        optionList(['PROD', 'STAGING', 'TEST', 'DEV'], f.env, 'Any') + '</select>') +
      cell('<select class="' + cls('cyg-a-fsel', f.outcome) + '" data-filter="outcome" aria-label="Filter by outcome">' +
        optionList(['allowed', 'denied', 'failed'], f.outcome, 'Any') + '</select>') +
      cell('') +
      '</tr>';
  }

  // Gap rows are interleaved by timestamp rather than appended, so a pause
  // appears in the timeline where it happened. They are only drawn on an
  // unfiltered-by-category view: a gap row inside a list filtered to
  // "connections" would be claiming something about connections that it is
  // not saying.
  function buildRows() {
    if (!state.events.length) {
      return '<tr><td colspan="9" class="cyg-a-empty">' +
        (state.error ? esc(state.error) : 'No events match these filters.') + '</td></tr>';
    }
    var items = state.events.map(function (e) {
      return { ts: Date.parse(e.occurredAt), gap: null, ev: e };
    });
    // Gap rows only on an unfiltered list. A gap row inside a list filtered
    // to "connections" would be claiming something about connections that it
    // is not saying.
    var f = state.filters;
    if (!f.category && !f.actor && !f.action && !f.target && !f.outcome && !f.env && !f.q) {
      var oldest = items.length ? items[items.length - 1].ts : 0;
      (state.status.gaps || []).forEach(function (g) {
        var at = Date.parse(g.to || g.from);
        if (at >= oldest) items.push({ ts: at, gap: g, ev: null });
      });
    }
    items.sort(function (a, b) { return b.ts - a.ts; });
    return items.map(function (it) { return it.gap ? gapRow(it.gap) : eventRow(it.ev); }).join('');
  }

  function gapRow(g) {
    var what = g.kind === 'off' ? 'Capture was <b>off</b>' : 'Capture was <b>paused</b>';
    var when = g.open
      ? 'from ' + esc(fmt(g.from)) + ' — still ' + (g.kind === 'off' ? 'off' : 'paused')
      : esc(fmt(g.from)) + ' &rarr; ' + esc(fmt(g.to));
    return '<tr class="gap' + (g.kind === 'off' ? ' off' : '') + '"><td colspan="9">' +
      what + ' ' + when + (g.by ? ' by ' + esc(g.by) : '') +
      (g.reason ? ' — "' + esc(g.reason) + '"' : '') +
      '. Security, access and PROD events in this window are still recorded and still shown.' +
      '</td></tr>';
  }

  // The category tag is the same bordered tag as the environment tag; a
  // PROD-scoped event takes the state-fail border and text. The avatar
  // blocks went — six categorical colours on a column of names was hue
  // spent on nothing.
  function categoryTag(e) {
    var cat = ((state.status.categories || []).filter(function (c) { return c.key === e.category; })[0]) || {};
    var word = e.category === 'prod' ? 'PROD' : (cat.label || e.category || '—');
    var prod = e.category === 'prod' || String(e.environment || '').toUpperCase() === 'PROD';
    return '<span class="cyg-a-cat' + (prod ? ' prod' : '') + '" title="' + esc(cat.label || e.category || '') + '">' + esc(word) + '</span>';
  }
  function eventRow(e) {
    var name = e.actorEmail || e.actorName || 'system';
    var roles = (e.effectiveRoles || []).join(' ');
    return '<tr class="ev" data-seq="' + e.seq + '" tabindex="0">' +
      '<td class="cyg-a-seq">' + esc(e.seq) + '</td>' +
      '<td class="cyg-a-when">' + esc(fmt(e.occurredAt)) + '<small>' + esc(rel(e.occurredAt)) + '</small></td>' +
      '<td><div class="cyg-a-who">' + esc(name) +
        (e.actorType === 'assistant' ? '<span class="cyg-a-ai">via Ask Cygenix</span>' : '') +
        (e.source === 'client' ? '<span class="cyg-a-src">client</span>' : '') +
        '<small>' + esc(roles || e.actorType) +
        (e.context && e.context.ip ? ' · ' + esc(e.context.ip) : '') + '</small></div></td>' +
      '<td>' + categoryTag(e) + '</td>' +
      '<td class="cyg-a-what"><div class="cyg-a-act">' + esc(e.action) + '</div>' +
        '<div class="s">' + esc(e.summary || '') + '</div></td>' +
      '<td class="cyg-a-tgt">' + esc((e.target && e.target.label) || e.resourceId || '—') + '</td>' +
      '<td><span class="cyg-a-env ' + esc(e.environment || '') + '">' + esc(e.environment || '—') + '</span></td>' +
      '<td><span class="cyg-a-out ' + esc(e.outcome || '') + '">' + esc(e.outcome) + '</span></td>' +
      '<td class="r cyg-a-srcw">' + (e.source === 'client' ? 'client' : 'server') + '</td></tr>';
  }

  // ── Drawer ──────────────────────────────────────────────────────────────

  function openDrawer(seq) {
    var e = state.events.filter(function (x) { return String(x.seq) === String(seq); })[0];
    if (!e) return;
    state.selected = String(seq);
    ensureDrawer();
    var cat = ((state.status.categories || []).filter(function (c) { return c.key === e.category; })[0]) || {};

    var html = '<dl class="cyg-a-kv">' +
      '<dt>When</dt><dd>' + esc(new Date(e.occurredAt).toLocaleString(undefined,
        { dateStyle: 'full', timeStyle: 'medium' })) +
        '<br><span style="font-family:var(--mono);font-size:11.5px;color:var(--text3)">' +
        esc(e.occurredAt) + '</span></dd>' +
      '<dt>Who</dt><dd>' + esc(e.actorName || e.actorEmail || 'system') +
        (e.actorEmail ? ' &lt;' + esc(e.actorEmail) + '&gt;' : '') +
        (e.effectiveRoles && e.effectiveRoles.length ? ' · ' + esc(e.effectiveRoles.join(', ')) : '') +
        (e.actorType === 'assistant'
          ? '<br><span class="cyg-a-ai">Ask Cygenix acted on behalf of ' + esc(e.onBehalfOf || '—') + '</span>'
          : '') + '</dd>' +
      '<dt>Outcome</dt><dd><span class="cyg-a-out ' + esc(e.outcome || '') + '">' + esc(e.outcome) +
        '</span>' + (e.detail && e.detail.reason ? ' — ' + esc(e.detail.reason) : '') + '</dd>' +
      '<dt>Target</dt><dd>' + esc((e.target && e.target.label) || e.resourceId || '—') +
        (e.resourceType ? ' <span style="color:var(--text3)">(' + esc(e.resourceType) + ')</span>' : '') + '</dd>' +
      '<dt>Environment</dt><dd>' + esc(e.environment || '—') + '</dd>' +
      '<dt>Category</dt><dd>' + esc(cat.label || e.category) +
        (cat.alwaysOn ? ' <span class="cyg-a-pill cyg-a-p-grey">always on</span>' : '') + '</dd>' +
      '<dt>Severity</dt><dd>' + esc(e.severity || 'info') + '</dd>' +
      '<dt>Recorded by</dt><dd>' + (e.source === 'client'
        ? 'the browser, and asserted by the signed-in user'
        : 'the server, which observed it directly') + '</dd>' +
      (e.context && e.context.ip ? '<dt>IP</dt><dd>' + esc(e.context.ip) + '</dd>' : '') +
      (e.context && e.context.userAgent ? '<dt>Client</dt><dd>' + esc(e.context.userAgent) + '</dd>' : '') +
      '<dt>Event ID</dt><dd style="font-family:var(--mono);font-size:12px">' + esc(e.id || '—') + '</dd>' +
      '<dt>Sequence</dt><dd style="font-family:var(--mono);font-size:12px">#' + esc(e.seq) + '</dd>' +
      '</dl>';

    if (e.changes && e.changes.length) {
      html += '<h4>CHANGES</h4><div class="cyg-a-diff">' +
        '<div class="row hd"><div>FIELD</div><div>BEFORE</div><div>AFTER</div></div>' +
        e.changes.map(function (c) {
          return '<div class="row"><div>' + esc(c.field) + '</div>' +
            '<div class="b">' + esc(fmtValue(c.before)) + '</div>' +
            '<div class="a">' + esc(fmtValue(c.after)) + '</div></div>';
        }).join('') + '</div>';
    }

    // The chain block is the reason this screen can be called evidence, so
    // it is shown in full rather than truncated to a friendly prefix. A
    // truncated hash cannot be checked against anything.
    html += '<h4>CHAIN</h4><div class="cyg-a-hash">' +
      'prev&nbsp;&nbsp;' + esc(e.prevHash || '(first entry)') + '<br>' +
      'hash&nbsp;&nbsp;<b>' + esc(e.entryHash) + '</b><br>' +
      '<span class="cyg-a-pill cyg-a-p-grey" style="margin-top:6px">' +
      'Verify chain, in the header, walks the whole chain</span></div>';

    document.getElementById('cyg-a-dact').textContent = e.action;
    document.getElementById('cyg-a-dsum').textContent = e.summary || '';
    document.getElementById('cyg-a-dbody').innerHTML = html;
    document.getElementById('cyg-a-drawer').classList.add('on');
    document.getElementById('cyg-a-scrim').classList.add('on');
    document.getElementById('cyg-a-dclose').focus();
    markSelected();
  }

  function fmtValue(v) {
    if (v === null || v === undefined) return '—';
    if (typeof v === 'object') { try { return JSON.stringify(v); } catch (e) { return String(v); } }
    return String(v);
  }

  function markSelected() {
    var rows = document.querySelectorAll('#cyg-a-rows tr.ev');
    for (var i = 0; i < rows.length; i++) {
      rows[i].classList.toggle('sel', rows[i].dataset.seq === state.selected);
    }
  }

  function closeDrawer() {
    var d = document.getElementById('cyg-a-drawer');
    if (d) d.classList.remove('on');
    var sc = document.getElementById('cyg-a-scrim');
    if (sc) sc.classList.remove('on');
    var prev = state.selected;
    state.selected = null;
    markSelected();
    // Return focus where it came from, so a keyboard reader is not dumped at
    // the top of a megabyte of document every time they close a row.
    var row = prev && document.querySelector('#cyg-a-rows tr.ev[data-seq="' + prev + '"]');
    if (row) row.focus();
  }

  function ensureDrawer() {
    if (document.getElementById('cyg-a-drawer')) return;
    var scrim = document.createElement('div');
    scrim.id = 'cyg-a-scrim';
    scrim.className = 'cyg-a-scrim';
    scrim.addEventListener('click', closeDrawer);
    var d = document.createElement('aside');
    d.id = 'cyg-a-drawer';
    d.className = 'cyg-a-drawer';
    d.setAttribute('aria-label', 'Event detail');
    d.innerHTML = '<div class="cyg-a-dh"><div><h2 id="cyg-a-dact"></h2><p id="cyg-a-dsum"></p></div>' +
      '<button class="cyg-a-x" id="cyg-a-dclose" type="button" aria-label="Close">&times;</button></div>' +
      '<div class="cyg-a-db" id="cyg-a-dbody"></div>';
    document.body.appendChild(scrim);
    document.body.appendChild(d);
    document.getElementById('cyg-a-dclose').addEventListener('click', closeDrawer);
  }

  // ── Settings tab ────────────────────────────────────────────────────────

  // ── Sign-ins tab ────────────────────────────────────────────────────────
  //
  // Every interactive sign-in, with the address and the place it came from.
  // Recorded by netlify/edge-functions/login-audit.js — the edge is the only
  // part of the stack that can see a city — and stored in the Cosmos `audit`
  // container, which is why this tab talks to the data proxy while the rest
  // of this screen talks to the audit function.
  //
  // Two scopes. Your own history is not an administrative privilege: seeing
  // a sign-in from a city you have never been to is the reason this exists,
  // and a control only an administrator can look at cannot do that job. The
  // whole organisation's is admin-gated on the server, and the toggle here
  // simply disappears when the server says no.

  // The identity provider, in the words a person would use.
  function idpLabel(idp) {
    var s = String(idp || 'local').toLowerCase();
    if (s === 'local' || !s) return 'Password';
    if (s.indexOf('google') !== -1) return 'Google';
    if (s.indexOf('microsoft') !== -1 || s.indexOf('live.com') !== -1) return 'Microsoft';
    if (s.indexOf('facebook') !== -1) return 'Facebook';
    if (s.indexOf('apple') !== -1) return 'Apple';
    return idp;
  }

  // A user agent string is unreadable and the useful part of it is two words.
  // Order matters: Edge and Opera both claim to be Chrome, and Chrome claims
  // to be Safari, so the most specific name has to be tested first.
  function browserLabel(ua) {
    var s = String(ua || '');
    if (!s) return '—';
    var name = /Edg\//.test(s) ? 'Edge'
      : /OPR\/|Opera/.test(s) ? 'Opera'
      : /Firefox\//.test(s) ? 'Firefox'
      : /Chrome\//.test(s) ? 'Chrome'
      : /Safari\//.test(s) ? 'Safari'
      : '';
    var os = /Windows/.test(s) ? 'Windows'
      : /Mac OS X|Macintosh/.test(s) ? 'macOS'
      : /Android/.test(s) ? 'Android'
      : /iPhone|iPad|iOS/.test(s) ? 'iOS'
      : /Linux/.test(s) ? 'Linux'
      : '';
    if (!name && !os) return 'Unknown';
    return (name + (name && os ? ' on ' : '') + os) || 'Unknown';
  }

  // City, region and country, skipping whichever are missing, and never
  // repeating a region that is just the city again.
  function placeLabel(r) {
    var parts = [];
    if (r.city) parts.push(r.city);
    if (r.region && r.region !== r.city) parts.push(r.region);
    if (r.country && r.country !== r.region) parts.push(r.country);
    return parts.length ? parts.join(', ') : 'Unknown location';
  }

  function loadSignins(opts) {
    var o = opts || {};
    var s = state.signins;
    if (s.loading) return;                       // one request at a time
    var api2 = root.CygenixDataApi;
    if (!api2 || typeof api2.callResult !== 'function') {
      s.error = 'The data layer is not loaded on this page.';
      s.loaded = true; renderSigninsPanel(); return;
    }
    // `denied` is deliberately NOT cleared here. A refused scope=all is
    // followed immediately by a fallback load of the caller's own history,
    // and clearing the flag at the top of that second load would erase the
    // sentence explaining why the scope changed — the reader would press
    // Everyone, see their own rows, and be told nothing. It is cleared when
    // they press a scope button, which is the point at which they have
    // asked a new question.
    s.loading = true; s.error = null;
    renderSigninsPanel();

    var scope = o.scope || s.scope;
    var days = o.days || s.days;
    api2.callResult('audit-signins', { method: 'GET', query: { scope: scope, days: days } })
      .then(function (r) {
        if (r.ok) {
          var d = r.data || {};
          s.rows = d.signins || [];
          s.scope = d.scope || scope;
          s.days = d.days || days;
          return;
        }
        // A refused scope=all is not an error to shout about: it means this
        // account is not an administrator, which is a normal answer. Fall
        // back to their own history rather than showing them a failure.
        var code = (r.error && r.error.status) || 0;
        if (scope === 'all' && (code === 403 || code === 401)) {
          s.denied = true; s.scope = 'mine';
          return;
        }
        s.error = (r.error && r.error.message) || 'Could not load the sign-in history.';
      })
      .catch(function (e) { s.error = e && e.message ? e.message : String(e); })
      .then(function () {
        s.loading = false; s.loaded = true;
        // The refused case re-asks for the narrower scope, once. triedAll
        // stops that becoming a loop if the server keeps refusing.
        if (s.denied && !s.triedAll) { s.triedAll = true; renderSigninsPanel(); loadSignins({ scope: 'mine' }); return; }
        renderSigninsPanel();
      });
  }

  function signinsTableHtml() {
    var s = state.signins;
    var all = s.scope === 'all';
    var head = '<tr>' +
      '<th>When</th>' +
      (all ? '<th>Who</th>' : '') +
      '<th>Where</th><th>IP address</th><th>Signed in with</th><th>Browser</th></tr>';
    if (!s.rows.length) {
      return '<table class="cyg-a-table"><thead>' + head + '</thead><tbody>' +
        '<tr><td colspan="' + (all ? 6 : 5) + '" class="cyg-a-empty">' +
        (s.days === 30 ? 'No sign-ins recorded yet.' : 'No sign-ins in this period.') +
        '<div style="margin-top:6px;font-size:13px">Sign-ins are recorded from the moment the feature ' +
        'was deployed. Anything before that is not here.</div></td></tr></tbody></table>';
    }
    var body = s.rows.map(function (r) {
      return '<tr>' +
        '<td class="cyg-a-when">' + esc(fmt(r.timestamp)) +
          '<div style="color:var(--color-neutral-700);font-size:12px">' + esc(rel(r.timestamp)) + '</div></td>' +
        (all ? '<td>' + esc(r.email || r.userId || '—') + '</td>' : '') +
        '<td>' + esc(placeLabel(r)) +
          (r.countryCode ? '<span class="cyg-a-cc">' + esc(r.countryCode) + '</span>' : '') + '</td>' +
        '<td class="cyg-a-seq">' + esc(r.ip || '—') + '</td>' +
        '<td>' + esc(idpLabel(r.idp)) + '</td>' +
        '<td>' + esc(browserLabel(r.userAgent)) + '</td>' +
        '</tr>';
    }).join('');
    return '<table class="cyg-a-table"><thead>' + head + '</thead><tbody>' + body + '</tbody></table>';
  }

  function renderSigninsPanel() {
    var el = document.getElementById('cyg-a-panel-signins');
    if (!el) return;
    var s = state.signins;

    if (!s.loaded && !s.loading) {
      el.innerHTML = '<div class="cyg-a-empty">Open this tab to load the sign-in history.</div>';
      return;
    }

    var places = {}, people = {};
    s.rows.forEach(function (r) {
      places[placeLabel(r)] = 1;
      people[r.email || r.userId || '?'] = 1;
    });
    var nPlaces = Object.keys(places).length;

    var summary = s.loading ? 'Loading…'
      : s.rows.length + (s.rows.length === 1 ? ' sign-in' : ' sign-ins') +
        ' in the last ' + s.days + ' days' +
        (nPlaces ? ', from ' + nPlaces + (nPlaces === 1 ? ' place' : ' places') : '') +
        (s.scope === 'all' ? ', across ' + Object.keys(people).length + ' people' : '');

    var toolbar = '<div class="cyg-a-toolbar">' +
      '<div class="cyg-a-seg" role="group" aria-label="Whose sign-ins">' +
        '<button type="button" data-si-scope="mine" class="' + (s.scope !== 'all' ? 'on' : '') +
          '" aria-pressed="' + (s.scope !== 'all') + '">Mine</button>' +
        '<button type="button" data-si-scope="all" class="' + (s.scope === 'all' ? 'on' : '') +
          '" aria-pressed="' + (s.scope === 'all') + '">Everyone</button>' +
      '</div>' +
      '<select id="cyg-a-si-days" aria-label="Period">' +
        [7, 30, 90, 365].map(function (d) {
          return '<option value="' + d + '"' + (s.days === d ? ' selected' : '') + '>Last ' + d + ' days</option>';
        }).join('') +
      '</select>' +
      '<button type="button" id="cyg-a-si-refresh"' + (s.loading ? ' disabled' : '') + '>Refresh</button>' +
      '<span class="cyg-a-count">' + esc(summary) + '</span>' +
      '</div>';

    var notes = '';
    if (s.denied) {
      notes += '<div class="cyg-a-note">Only an administrator can see everyone\'s sign-ins. ' +
        'Showing your own.</div>';
    }
    if (s.error) {
      notes += '<div class="cyg-a-note err">' + esc(s.error) + '</div>';
    }

    el.innerHTML = toolbar + notes +
      (s.loading && !s.rows.length ? '<div class="cyg-a-empty">Loading the sign-in history…</div>' : signinsTableHtml()) +
      '<div class="cyg-a-note">Recorded at the edge, which is the only part of the stack that can see a ' +
      'city. A missing location means the address could not be placed — a corporate network or a VPN — ' +
      'and the sign-in is recorded either way. Entries are kept for 30 days.' +
      // The integrity band at the top of this screen describes the hash
      // chain, and these rows are not in it. Leaving that unsaid would let a
      // reader carry "verified" across from the band to this table, which is
      // exactly the kind of unearned assurance an audit screen must not give.
      '<br><b>These entries are stored separately from the hash-chained trail above and are not covered ' +
      'by its verification.</b></div>';

    wireSignins();
  }

  function wireSignins() {
    var panel = document.getElementById('cyg-a-panel-signins');
    if (!panel) return;
    panel.querySelectorAll('[data-si-scope]').forEach(function (b) {
      b.addEventListener('click', function () {
        var next = b.getAttribute('data-si-scope');
        if (next === state.signins.scope) return;          // already there
        state.signins.triedAll = false;
        state.signins.denied = false;
        state.signins.scope = next;
        loadSignins({ scope: next });
      });
    });
    var days = panel.querySelector('#cyg-a-si-days');
    if (days) days.addEventListener('change', function () {
      state.signins.days = parseInt(days.value, 10) || 30;
      loadSignins({ days: state.signins.days });
    });
    var ref = panel.querySelector('#cyg-a-si-refresh');
    if (ref) ref.addEventListener('click', function () { loadSignins({}); });
  }

  function renderSettingsPanel() {
    var el = document.getElementById('cyg-a-panel-settings');
    if (!el) return;
    var st = state.status;
    var cfg = st.settings;
    var can = st.canConfigure;

    var cats = (st.categories || []).map(function (c) {
      var on = cfg.categories[c.key] !== false;
      if (c.alwaysOn) {
        return '<div class="cyg-a-cat-row"><div class="t"><b>' + esc(c.label) + '</b>' +
          '<small>' + esc(c.detail || '') + ' Always recorded — a pause cannot hide it and no role can switch it off.</small></div>' +
          '<span class="cyg-a-locked" title="Locked: recorded whatever the capture state">Locked</span></div>';
      }
      return '<div class="cyg-a-cat-row"><div class="t"><b>' + esc(c.label) +
        '</b><small>' + esc(c.detail || '') + '</small></div>' +
        '<label class="cyg-a-tg"><input type="checkbox" data-cat-key="' + esc(c.key) + '"' +
        (on ? ' checked' : '') + (can ? '' : ' disabled') +
        ' aria-label="Record ' + esc(c.label) + '"><span></span></label></div>';
    }).join('');

    var retention = [[90, '90 days'], [365, '1 year'], [2555, '7 years']].map(function (o) {
      return '<label><input type="radio" name="cyg-a-ret" value="' + o[0] + '"' +
        (cfg.retentionDays === o[0] ? ' checked' : '') + (can ? '' : ' disabled') + '>' + o[1] + '</label>';
    }).join('');

    var pauseMax = [[240, 'Max 4 hours'], [1440, 'Max 24 hours']].map(function (o) {
      return '<label><input type="radio" name="cyg-a-pmax" value="' + o[0] + '"' +
        (cfg.pauseMaxMinutes === o[0] ? ' checked' : '') + (can ? '' : ' disabled') + '>' + o[1] + '</label>';
    }).join('');

    var flag = function (key, label, detail) {
      return '<div class="cyg-a-cat-row"><div class="t"><b>' + esc(label) + '</b><small>' + esc(detail) +
        '</small></div><label class="cyg-a-tg"><input type="checkbox" data-flag="' + key + '"' +
        (cfg[key] !== false ? ' checked' : '') + (can ? '' : ' disabled') +
        ' aria-label="' + esc(label) + '"><span></span></label></div>';
    };

    el.innerHTML =
      (can ? '' : '<div class="cyg-a-banner">You can see how capture is configured but not change it. ' +
        'Changing it belongs to the Organisation Owner and the Platform Administrator — an auditor who ' +
        'can quieten the trail they report on is not an auditor.</div>') +
      '<div class="cyg-a-grid2"><div class="cyg-a-card cyg-a-pad">' +
      '<h3>What gets recorded</h3><p>Turn categories off to reduce noise. The four locked rows ' +
      'cannot be disabled here or through the API — they are what a pause is not allowed to hide.</p>' +
      '<div id="cyg-a-cats">' + cats + '</div></div>' +
      '<div style="display:flex;flex-direction:column;gap:16px">' +
      '<div class="cyg-a-card cyg-a-pad"><h3>Retention</h3>' +
      '<p>Older events are archived and then purged. A purge writes its own entry and leaves a signed ' +
      'checkpoint, so verification still works across the boundary.</p>' +
      '<div class="cyg-a-radio" id="cyg-a-ret">' + retention + '</div>' +
      flag('archiveBeforePurge', 'Archive before purging',
           'On: expired entries are copied to an archive and removed from the live log — ' +
           'the organisation still holds them. Off: they are permanently erased.') +
      '<div class="cyg-a-note">Runs nightly. ' +
      (cfg.archiveBeforePurge
        ? 'Nothing is destroyed: expired entries move to an archive and out of the live log.'
        : '<b>Archiving is off, so expired entries are permanently erased.</b> That is real ' +
          'deletion and cannot be undone.') +
      ' Every purge writes its own entry and leaves a checkpoint, so verification still ' +
      'works across the boundary.</div></div>' +
      '<div class="cyg-a-card cyg-a-pad"><h3>Pause ceiling</h3>' +
      '<p>A pause always has an end, so nobody can forget to switch it back on.</p>' +
      '<div class="cyg-a-radio" id="cyg-a-pmax">' + pauseMax + '</div>' +
      '<div class="cyg-a-note">Turning the log <b>Off</b> needs a reason and the word OFF typed out. ' +
      'Off has no timer. Administrators are notified in-app rather than by email — there are no ' +
      'server-held mail credentials, and saying so is better than reporting a message that never left.' +
      '</div></div>' +
      '<div class="cyg-a-card cyg-a-pad"><h3>Detail level</h3>' +
      '<p>Passwords, keys, tokens and connection strings are redacted before storage whatever these say. ' +
      'The chain is append-only, so a secret written into it could not be removed afterwards.</p>' +
      flag('storeDiffs', 'Store before and after values', 'A field-level diff on every change') +
      flag('storeIp', 'Store IP address and browser', 'Needed for security reviews') +
      flag('recordAssistant', 'Record Ask Cygenix actions',
           'AI actions stored with the person they acted for') +
      '</div></div></div>';

    wireSettings();
  }

  // ── Integrity tab ───────────────────────────────────────────────────────

  function renderIntegrityPanel() {
    var el = document.getElementById('cyg-a-panel-integrity');
    if (!el) return;
    var last5 = state.events.slice(0, 5).reverse();
    var blocks = last5.length
      ? last5.map(function (e, i) {
          return (i ? '<span class="cyg-a-arrow">&rarr;</span>' : '') +
            '<div class="cyg-a-blk"><b>' + esc(e.action) + '</b>' +
            esc(String(e.entryHash || '').slice(0, 10)) + '…<br>' +
            '<span style="color:var(--text3)">prev ' +
            esc(String(e.prevHash || '').slice(0, 6) || '—') + '…</span></div>';
        }).join('')
      : '<span style="color:var(--text2);font-size:13px">No entries yet.</span>';

    var out = state.verifying
      ? 'Walking the chain…'
      : state.verifyResult
        ? (state.verifyResult.ok
            ? 'Verified ' + esc(rel(state.verifyResult.verifiedAt)) + ' — ' +
              esc(state.verifyResult.count) + ' entries, no breaks ' +
              '<span class="cyg-a-pill cyg-a-p-green">intact</span>'
            : '<span class="cyg-a-pill cyg-a-p-red">BROKEN</span> at entry #' +
              esc(state.verifyResult.brokenAt) + ' — ' + esc(state.verifyResult.reason))
        : 'Not verified in this session.';

    el.innerHTML =
      '<div class="cyg-a-card cyg-a-pad">' +
      '<h3>Hash chain</h3>' +
      '<p>Every entry stores the SHA-256 of the one before it, so editing or removing any past row ' +
      'breaks every hash after it. Verification walks the chain and names the first entry that does ' +
      'not line up.</p>' +
      '<div class="cyg-a-chain">' + blocks + '</div>' +
      '<div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">' +
      '<span id="cyg-a-verifyout" style="font-size:14px;color:var(--color-neutral-800)">' + out + '</span>' +
      (state.status.canVerify ? '<span class="cyg-a-note-h">— Verify chain is in the header.</span>' : '') + '</div>' +
      '<div class="cyg-a-note warn" style="margin-top:14px"><b>The known limit, stated rather than buried.</b> ' +
      'Netlify Blobs has no transactions, so two appends at the same instant can race for the head ' +
      'position. A short retry closes the realistic window at this product\'s traffic, and verification ' +
      'reports a break rather than hiding one — but this is tamper-evident, not tamper-proof, and an ' +
      'evidence pack should say so.</div></div>';
  }

  // Retention has its own tab: what it has actually done, and the control
  // to run it. The policy (how long, archive or erase) stays under Capture
  // settings with the other settings, because it is a setting.
  function renderRetentionPanel() {
    var el = document.getElementById('cyg-a-panel-retention');
    if (!el) return;
    el.innerHTML = retentionHtml();
    var pb = document.getElementById('cyg-a-purge');
    if (pb) pb.addEventListener('click', runPurge);
  }

  // What retention has actually DONE, which is a different question from what
  // it is set to. A chain that starts at sequence 1 has never been purged,
  // and saying so is more use to a reader than repeating the policy at them.
  function retentionHtml() {
    var cp = state.status.checkpoint || { purged: false, chainStartsAt: 1 };
    var days = state.status.settings.retentionDays;
    var label = days === 2555 ? '7 years' : days === 90 ? '90 days' : '1 year';
    var canRun = state.status.canConfigure;

    var body = cp.purged
      ? '<p>Retention has run. The chain now begins at entry <b>#' + esc(cp.chainStartsAt) +
        '</b>; <b>' + esc(cp.purgedTotal) + '</b> older ' +
        (cp.purgedTotal === 1 ? 'entry has' : 'entries have') + ' been ' +
        (cp.archived ? 'archived and removed from the live log' : '<b>erased</b>') + '. ' +
        'Verification starts from the checkpoint left at that boundary, so everything ' +
        'kept is still verifiable.</p>' +
        '<div class="cyg-a-hash">checkpoint&nbsp;&nbsp;<b>' + esc(cp.anchorHash || '—') + '</b><br>' +
        'anchored at entry #' + esc(cp.chainStartsAt) +
        (cp.anchorAt ? ', ' + esc(fmt(cp.anchorAt)) : '') + '<br>' +
        'last purge ' + (cp.lastPurgeAt ? esc(fmt(cp.lastPurgeAt)) : '—') +
        (cp.archiveKey ? '<br>archive ' + esc(cp.archiveKey) : '') + '</div>'
      : '<p>Nothing has been purged yet. The chain still begins at its first entry, so ' +
        'verification walks the whole of it. Events older than <b>' + esc(label) +
        '</b> are purged nightly once any exist.</p>';

    return '<div class="cyg-a-card cyg-a-pad">' +
      '<h3>Retention</h3>' + body +
      '<div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:12px">' +
      (canRun
        ? '<button class="cyg-a-btn" id="cyg-a-purge" type="button"' +
          (state.purging ? ' disabled' : '') + '>' +
          (state.purging ? 'Running…' : 'Run retention now') + '</button>'
        : '') +
      '<span id="cyg-a-purgeout" style="font-size:14px;color:var(--color-neutral-800)">' +
      (state.purgeResult || 'Runs nightly. Each run is budgeted, so a large backlog is ' +
       'worked down over successive nights rather than in one pass.') +
      '</span></div>' +
      '<div class="cyg-a-note" style="margin-top:14px"><b>Why deleting does not break ' +
      'verification.</b> Every purge writes its own entry into the chain first, carrying a ' +
      'checkpoint of the first surviving entry\'s hash — so the checkpoint is attested by the ' +
      'chain that continues after it. Altering the checkpoint makes it disagree with that entry; ' +
      'altering the entry breaks every hash after it. There is no signing key involved, and none ' +
      'is claimed.</div></div>';
  }

  function runPurge() {
    if (!confirm('Run retention now?\n\nEvents older than the retention period are ' +
        (state.status.settings.archiveBeforePurge
          ? 'archived and then removed from the live log.'
          : 'PERMANENTLY ERASED — archiving is switched off.'))) return;
    state.purging = true;
    state.purgeResult = null;
    renderRetentionPanel();
    post({ op: 'purge' }).then(function (d) {
      state.purgeResult = d.purged
        ? 'Purged ' + d.purged + ' entr' + (d.purged === 1 ? 'y' : 'ies') +
          (d.archived ? ' (archived)' : ' (erased)') +
          (d.more ? ' — more remaining, the next run continues' : '')
        : 'Nothing to purge — ' + esc(d.reason || 'no entry has aged out');
      toast(d.purged ? 'Purged ' + d.purged : 'Nothing to purge');
      return load();
    }).catch(function (e) {
      state.purgeResult = 'Failed: ' + esc(e.message);
      toast('Retention run failed: ' + e.message);
      state.purging = false;
      renderRetentionPanel();
    }).then(function () { state.purging = false; });
  }

  function runVerify() {
    state.verifying = true;
    renderIntegrityPanel();
    refreshBand();
    api('what=verify').then(function (d) {
      state.verifyResult = d;
      toast(d.ok ? 'Chain intact — ' + d.count + ' entries' : 'Chain BROKEN at entry ' + d.brokenAt);
    }).catch(function (e) {
      state.verifyResult = { ok: false, brokenAt: '?', reason: e.message };
      toast('Verification failed: ' + e.message);
    }).then(function () {
      state.verifying = false;
      renderIntegrityPanel();
      refreshBand();
    });
  }
  // The band and the header button both say what the chain is verified to,
  // so they are redrawn together after a verification.
  function refreshBand() {
    var band = mount && mount.querySelector('#cyg-a-band');
    if (band) {
      var holder = document.createElement('div');
      holder.innerHTML = bandHtml();
      band.replaceWith(holder.firstElementChild);
    }
    var head = mount && mount.querySelector('.cyg-a-head');
    if (head) {
      var h = document.createElement('div');
      h.innerHTML = headerHtml();
      head.replaceWith(h.firstElementChild);
      wireHeader();
    }
  }

  // ── Modals ──────────────────────────────────────────────────────────────

  function modal(html) {
    var m = document.getElementById('cyg-a-modal');
    if (!m) {
      m = document.createElement('div');
      m.id = 'cyg-a-modal';
      m.className = 'cyg-a-modal';
      m.innerHTML = '<div class="cyg-a-mbox" id="cyg-a-mbox" role="dialog" aria-modal="true"></div>';
      document.body.appendChild(m);
      m.addEventListener('click', function (e) { if (e.target === m) closeModal(); });
      // ONE listener for the life of the page, dispatching to whichever
      // modal is currently open. Re-attaching on every open stacked handlers
      // and made the second pause post itself twice.
      document.getElementById('cyg-a-mbox').addEventListener('click', function (e) {
        if (modalSubmit) modalSubmit(e);
      });
    }
    document.getElementById('cyg-a-mbox').innerHTML = html;
    m.classList.add('on');
    var first = m.querySelector('textarea, select, input');
    if (first) first.focus();
  }

  function closeModal() {
    var m = document.getElementById('cyg-a-modal');
    if (m) m.classList.remove('on');
  }

  function askPause() {
    var max = state.status.settings.pauseMaxMinutes;
    var opts = (state.status.pausePresets || [30, 60, 120, 240])
      .filter(function (m) { return m <= max; })
      .map(function (m) {
        return '<option value="' + m + '"' + (m === 120 ? ' selected' : '') + '>' +
          (m < 60 ? m + ' minutes' : (m / 60) + ' hour' + (m > 60 ? 's' : '')) +
          (m === max ? ' (max)' : '') + '</option>';
      }).join('');
    modal('<h3>Pause the audit log</h3>' +
      '<p>General change capture stops until the timer runs out. Security, access, PROD and the log\'s ' +
      'own events keep recording.</p>' +
      '<label for="cyg-a-dur">Pause for</label><select id="cyg-a-dur">' + opts + '</select>' +
      '<label for="cyg-a-why">Reason <span style="color:var(--state-fail)">*</span></label>' +
      '<textarea id="cyg-a-why" placeholder="e.g. bulk re-import of 40 staging tables, too noisy"></textarea>' +
      '<div class="cyg-a-note">This is recorded as <span style="font-family:var(--mono)">audit.pause</span> ' +
      'with your name and your reason, and drawn as a gap in the timeline. The log records its own ' +
      'blindness.</div>' +
      '<div class="cyg-a-mact"><button class="cyg-a-btn" type="button" data-m="cancel">Cancel</button>' +
      '<button class="cyg-a-btn warn" type="button" data-m="pause">Pause</button></div>');
    wireModal();
  }

  function askOff() {
    modal('<h3>Turn the audit log off?</h3>' +
      '<p>General change capture stops with <b>no end time</b>. Security, access, PROD and the log\'s ' +
      'own events keep recording, and it stays off until somebody turns it back on.</p>' +
      '<label for="cyg-a-why">Reason <span style="color:var(--state-fail)">*</span></label>' +
      '<textarea id="cyg-a-why" placeholder="Why does it need to be off?"></textarea>' +
      '<label for="cyg-a-conf">Type <b>OFF</b> to confirm</label>' +
      '<input type="text" id="cyg-a-conf" autocomplete="off" spellcheck="false">' +
      '<div class="cyg-a-note">Administrators are notified in the app. Email notification is not ' +
      'available — there are no server-held mail credentials.</div>' +
      '<div class="cyg-a-mact"><button class="cyg-a-btn" type="button" data-m="cancel">Cancel</button>' +
      '<button class="cyg-a-btn danger" type="button" data-m="off">Turn off</button></div>');
    wireModal();
  }

  function wireModal() {
    modalSubmit = function (ev) {
      var m = ev.target.dataset && ev.target.dataset.m;
      if (!m) return;
      if (m === 'cancel') return closeModal();
      var why = (document.getElementById('cyg-a-why').value || '').trim();
      if (why.length < 4) {
        document.getElementById('cyg-a-why').style.borderColor = 'var(--state-fail)';
        document.getElementById('cyg-a-why').focus();
        return;
      }
      var body = { op: 'status', reason: why };
      if (m === 'pause') {
        body.state = 'paused';
        body.pauseMinutes = parseInt(document.getElementById('cyg-a-dur').value, 10);
      } else {
        body.state = 'off';
        body.confirm = (document.getElementById('cyg-a-conf').value || '').trim();
        if (body.confirm.toUpperCase() !== 'OFF') {
          document.getElementById('cyg-a-conf').style.borderColor = 'var(--state-fail)';
          document.getElementById('cyg-a-conf').focus();
          return;
        }
      }
      ev.target.disabled = true;
      post(body).then(function (d) {
        closeModal();
        toast(m === 'pause' ? 'Paused — it resumes by itself' : 'Capture off — administrators notified in-app');
        if (d.notify && d.notify.adminsEmailed === false) {
          console.warn('[audit] ' + d.notify.reason);
        }
        return load();
      }).catch(function (e) {
        ev.target.disabled = false;
        toast('Could not change capture state: ' + e.message);
      });
    };
  }

  function setCaptureState(s) {
    if (!state.status.canConfigure || s === state.status.state) return;
    if (s === 'paused') return askPause();
    if (s === 'off') return askOff();
    post({ op: 'status', state: 'recording' }).then(function () {
      toast('Recording resumed');
      return load();
    }).catch(function (e) { toast('Could not resume: ' + e.message); });
  }

  // ── Settings writes ─────────────────────────────────────────────────────

  function saveSettings(patch, label) {
    return post(Object.assign({ op: 'settings' }, patch)).then(function (d) {
      state.status.settings = d.settings;
      toast(label);
      renderSettingsPanel();
      // The card shows "N of M categories on", so it goes stale the moment a
      // toggle moves. refreshStatus() re-wires as well as re-renders: an
      // earlier version replaced the node with outerHTML and silently took
      // the capture-state buttons' click handler with it, which the browser
      // smoke caught and a static test never would have.
      refreshStatus();
    }).catch(function (e) {
      toast('Not changed: ' + e.message);
      renderSettingsPanel();   // snap the control back to the stored truth
    });
  }

  // ── Export ──────────────────────────────────────────────────────────────
  //
  // The endpoint needs an Authorization header, which a plain <a download>
  // cannot send, so the file is fetched and handed to the browser as a blob.
  function exportAs(format) {
    var t = token();
    if (!t) return;
    toast('Preparing the ' + format.toUpperCase() + '…');
    fetch(ENDPOINT + '?what=export&format=' + format + '&' + filterQuery(),
      { headers: { Authorization: 'Bearer ' + t } })
      .then(function (r) {
        if (!r.ok) return r.json().then(function (d) { throw new Error(d.error || ('HTTP ' + r.status)); });
        return r.blob();
      })
      .then(function (blob) {
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = 'cygenix_audit_' + new Date().toISOString().slice(0, 10) + '.' + format;
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(function () { URL.revokeObjectURL(url); }, 1000);
        toast('Exported — the export is itself in the log');
        // The export wrote an audit.export entry, so the list the user is
        // looking at is now one row out of date.
        reloadEvents();
      })
      .catch(function (e) { toast('Export failed: ' + e.message); });
  }

  // ── Wiring ──────────────────────────────────────────────────────────────

  // Replacing a node throws away its listeners, so re-rendering the status
  // card and re-wiring it are one operation and never two.
  function refreshStatus() {
    var card = mount && mount.querySelector('.cyg-a-status');
    if (!card) return;
    var holder = document.createElement('div');
    holder.innerHTML = statusHtml();
    card.replaceWith(holder.firstElementChild);
    wireStatus();
  }

  function wireStatus() {
    var seg = document.getElementById('cyg-a-state-seg');
    if (!seg) return;
    seg.addEventListener('click', function (e) {
      var b = e.target.closest('button[data-s]');
      if (b && !b.disabled) setCaptureState(b.dataset.s);
    });
  }

  function wireHeader() {
    var csv = document.getElementById('cyg-a-csv');
    if (csv) csv.addEventListener('click', function () { exportAs('csv'); });
    var json = document.getElementById('cyg-a-json');
    if (json) json.addEventListener('click', function () { exportAs('json'); });
    var vb = document.getElementById('cyg-a-verify');
    if (vb) vb.addEventListener('click', runVerify);
  }

  function wire() {
    wireStatus();
    wireHeader();
    var tabs = mount.querySelectorAll('.cyg-a-tab');
    for (var i = 0; i < tabs.length; i++) {
      tabs[i].addEventListener('click', function (e) { selectTab(e.currentTarget.dataset.tab); });
      tabs[i].addEventListener('keydown', onTabKey);
    }
  }

  function onTabKey(e) {
    var keys = { ArrowRight: 1, ArrowLeft: -1, Home: 'first', End: 'last' };
    if (!(e.key in keys)) return;
    e.preventDefault();
    var idx = TABS.map(function (t) { return t.key; }).indexOf(state.tab);
    var next = keys[e.key] === 'first' ? 0
      : keys[e.key] === 'last' ? TABS.length - 1
      : (idx + keys[e.key] + TABS.length) % TABS.length;
    selectTab(TABS[next].key);
    var el = document.getElementById('cyg-a-tab-' + TABS[next].key);
    if (el) el.focus();
  }

  function selectTab(key) {
    state.tab = key;
    // The sign-in history is a second backend and a second round trip, so it
    // is fetched when somebody first asks to see it rather than on every
    // visit to this screen. Guarded on `loaded` AND on `loading`, so opening
    // the tab twice while the first request is in flight does not start a
    // second one, and a tab that loaded and found nothing does not re-ask on
    // every click. Refresh is the way to look again.
    if (key === 'signins' && !state.signins.loaded && !state.signins.loading) loadSignins({});
    TABS.forEach(function (t) {
      var tab = document.getElementById('cyg-a-tab-' + t.key);
      var panel = document.getElementById('cyg-a-panel-' + t.key);
      if (tab) {
        tab.setAttribute('aria-selected', String(t.key === key));
        tab.tabIndex = t.key === key ? 0 : -1;
      }
      if (panel) panel.hidden = t.key !== key;
    });
  }

  function el2(sel) {
    var panel = document.getElementById('cyg-a-panel-events');
    return panel && panel.querySelector(sel);
  }

  function wireEvents() {
    var q = document.getElementById('cyg-a-q');
    if (q) {
      q.addEventListener('input', function (e) {
        state.filters.q = e.target.value;
        clearTimeout(searchTimer);
        // Debounced, because each keystroke is a blob read on the other end.
        searchTimer = setTimeout(reloadEvents, 350);
      });
    }
    // One delegated listener on the filter row rather than one per control,
    // because the row is rebuilt on every reload and per-control listeners
    // would have to be re-attached each time — the same mistake that cost the
    // capture-state buttons their handler.
    var frow = el2('.cyg-a-filters');
    if (frow) {
      frow.addEventListener('change', function (e) {
        var key = e.target.dataset && e.target.dataset.filter;
        if (!key) return;
        state.filters[key] = key === 'days' ? (parseInt(e.target.value, 10) || 0) : e.target.value;
        reloadEvents();
      });
      frow.addEventListener('input', function (e) {
        // The free-text column filter debounces; the selects do not need to.
        if (!e.target.dataset || e.target.dataset.filter !== 'target') return;
        state.filters.target = e.target.value;
        clearTimeout(searchTimer);
        searchTimer = setTimeout(reloadEvents, 350);
      });
    }

    var clear = document.getElementById('cyg-a-clear');
    if (clear) {
      clear.addEventListener('click', function () {
        state.filters = { q: '', days: 7, actor: '', action: '', target: '',
                          outcome: '', env: '', category: '' };
        reloadEvents();
      });
    }

    var chips = document.getElementById('cyg-a-chips');
    if (chips) {
      chips.addEventListener('click', function (e) {
        var c = e.target.closest('[data-cat]');
        if (!c) return;
        state.filters.category = c.dataset.cat;
        reloadEvents();
      });
    }

    var rows = document.getElementById('cyg-a-rows');
    if (rows) {
      rows.addEventListener('click', function (e) {
        var r = e.target.closest('tr.ev');
        if (r) openDrawer(r.dataset.seq);
      });
      rows.addEventListener('keydown', function (e) {
        if (e.key !== 'Enter' && e.key !== ' ') return;
        var r = e.target.closest('tr.ev');
        if (r) { e.preventDefault(); openDrawer(r.dataset.seq); }
      });
    }

    var more = document.getElementById('cyg-a-more');
    if (more) more.addEventListener('click', loadMore);
  }

  function wireSettings() {
    var cats = document.getElementById('cyg-a-cats');
    if (cats) {
      cats.addEventListener('change', function (e) {
        var k = e.target.dataset.catKey;
        if (!k) return;
        var patch = { categories: {} };
        patch.categories[k] = e.target.checked;
        saveSettings(patch, (e.target.checked ? 'Now recording ' : 'Stopped recording ') + k);
      });
    }
    var ret = document.getElementById('cyg-a-ret');
    if (ret) {
      ret.addEventListener('change', function (e) {
        saveSettings({ retentionDays: parseInt(e.target.value, 10) }, 'Retention updated');
      });
    }
    var pm = document.getElementById('cyg-a-pmax');
    if (pm) {
      pm.addEventListener('change', function (e) {
        saveSettings({ pauseMaxMinutes: parseInt(e.target.value, 10) }, 'Pause ceiling updated');
      });
    }
    var panel = document.getElementById('cyg-a-panel-settings');
    if (panel) {
      panel.addEventListener('change', function (e) {
        var f = e.target.dataset.flag;
        if (!f) return;
        var patch = {};
        patch[f] = e.target.checked;
        saveSettings(patch, 'Detail level updated');
      });
    }
  }

  document.addEventListener('keydown', function (e) {
    if (e.key !== 'Escape') return;
    var m = document.getElementById('cyg-a-modal');
    if (m && m.classList.contains('on')) return closeModal();
    var d = document.getElementById('cyg-a-drawer');
    if (d && d.classList.contains('on')) closeDrawer();
  });

  // ── Public entry point ──────────────────────────────────────────────────

  root.CygenixAuditView = {
    // Called by dashboard-app.js's renderAuditLog(). Re-entrant: switching
    // away and back re-reads rather than showing a stale trail, because an
    // audit log that is five minutes behind is misleading in the one way
    // this screen cannot afford.
    render: function (el) {
      injectStyles();
      mount = el;
      state.loaded = false;
      state.facets = { actors: [], actions: [] };
      render();
      return load();
    },
    // Exposed for the test suite.
    _state: state,
  };

  if (typeof module === 'object' && module.exports) {
    module.exports = root.CygenixAuditView;
  }
})(typeof window !== 'undefined' ? window : globalThis);
