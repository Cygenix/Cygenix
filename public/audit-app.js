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
  var AVATAR_COLOURS = ['var(--accent)', 'var(--teal)', 'var(--purple)', 'var(--green)', 'var(--amber)', 'var(--red)'];
  function avatarColour(email) {
    var s = String(email || ''), h = 0;
    for (var i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
    return AVATAR_COLOURS[h % AVATAR_COLOURS.length];
  }

  function outcomeClass(o) {
    return o === 'allowed' ? 'cyg-a-p-green' : o === 'denied' ? 'cyg-a-p-red' : 'cyg-a-p-amber';
  }

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
      /* Every colour is a theme token, so this screen follows the workspace
         theme rather than pinning the light palette the mockup was drawn in. */
      '.cyg-a-card{background:var(--bg2);border:1px solid var(--border);border-radius:var(--r-lg);box-shadow:var(--shadow-soft)}',
      '.cyg-a-status{display:grid;grid-template-columns:1fr auto;gap:16px;padding:18px 20px;align-items:center;margin-bottom:16px}',
      '.cyg-a-status h3{margin:0 0 4px;font-size:15px;display:flex;align-items:center;gap:10px;flex-wrap:wrap}',
      '.cyg-a-status p{margin:0;color:var(--text2);font-size:13px}',
      '.cyg-a-status.paused{border-color:var(--amber);background:linear-gradient(0deg,var(--bg2),var(--amber-bg))}',
      '.cyg-a-status.off{border-color:var(--red);background:linear-gradient(0deg,var(--bg2),var(--red-bg))}',
      '.cyg-a-pill{display:inline-flex;align-items:center;gap:6px;font-size:11.5px;font-weight:600;border-radius:999px;padding:2px 9px;white-space:nowrap}',
      '.cyg-a-p-green{background:var(--green-bg);color:var(--green)}',
      '.cyg-a-p-amber{background:var(--amber-bg);color:var(--amber)}',
      '.cyg-a-p-red{background:var(--red-bg);color:var(--red)}',
      '.cyg-a-p-grey{background:var(--bg4);color:var(--text2)}',
      '.cyg-a-p-purple{background:var(--purple-bg);color:var(--purple)}',
      '.cyg-a-dot{width:7px;height:7px;border-radius:50%;background:currentColor;flex:none}',
      '.cyg-a-dot.live{animation:cygADot 1.6s infinite}',
      '@keyframes cygADot{0%{box-shadow:0 0 0 0 currentColor}70%{box-shadow:0 0 0 7px transparent}100%{box-shadow:0 0 0 0 transparent}}',
      '@media (prefers-reduced-motion:reduce){.cyg-a-dot.live{animation:none}}',
      '.cyg-a-always{margin-top:10px;font-size:12.5px;color:var(--text2);display:flex;gap:8px;align-items:flex-start}',
      '.cyg-a-always .ic{flex:none;margin-top:2px}',
      '.cyg-a-seg{display:inline-flex;background:var(--bg3);border:1px solid var(--border);border-radius:10px;padding:3px;gap:2px}',
      '.cyg-a-seg button{border:0;background:transparent;padding:7px 14px;border-radius:7px;cursor:pointer;font-weight:500;color:var(--text2);display:flex;align-items:center;gap:7px;font:inherit}',
      '.cyg-a-seg button:hover:not(:disabled){color:var(--text)}',
      '.cyg-a-seg button:disabled{opacity:.5;cursor:not-allowed}',
      '.cyg-a-seg button.on{background:var(--bg2);color:var(--text);box-shadow:var(--shadow-soft)}',
      '.cyg-a-seg button.on[data-s=recording]{color:var(--green)}',
      '.cyg-a-seg button.on[data-s=paused]{color:var(--amber)}',
      '.cyg-a-seg button.on[data-s=off]{color:var(--red)}',
      '.cyg-a-kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:16px}',
      '.cyg-a-kpi{padding:14px 16px}',
      '.cyg-a-kpi .l{font-size:12px;color:var(--text2)}',
      '.cyg-a-kpi .v{font-size:24px;font-weight:600;margin-top:2px;font-variant-numeric:tabular-nums}',
      '.cyg-a-kpi .d{font-size:12px;color:var(--text3)}',
      '.cyg-a-tabs{display:flex;gap:4px;border-bottom:1px solid var(--border);margin-bottom:14px;overflow-x:auto}',
      '.cyg-a-tab{border:0;background:none;padding:10px 14px;cursor:pointer;color:var(--text2);border-bottom:2px solid transparent;margin-bottom:-1px;font-weight:500;white-space:nowrap;font:inherit;text-align:left}',
      '.cyg-a-tab[aria-selected="true"]{color:var(--accent);border-color:var(--accent);font-weight:600}',
      '.cyg-a-tab small{display:block;font-weight:400;font-size:11px;color:var(--text3)}',
      '.cyg-a-toolbar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:12px}',
      '.cyg-a-in,.cyg-a-sel{height:34px;border:1px solid var(--border2);border-radius:8px;background:var(--bg2);color:var(--text);padding:0 10px;font:inherit;font-size:13px}',
      '.cyg-a-in{min-width:200px;flex:1;max-width:340px}',
      '.cyg-a-btn{height:34px;border:1px solid var(--border2);border-radius:8px;background:var(--bg2);color:var(--text);padding:0 12px;cursor:pointer;display:inline-flex;align-items:center;gap:6px;font-weight:500;font-size:13px;font-family:inherit}',
      '.cyg-a-btn:hover:not(:disabled){border-color:var(--text3)}',
      '.cyg-a-btn:disabled{opacity:.55;cursor:not-allowed}',
      '.cyg-a-btn.primary{background:var(--accent);border-color:var(--accent);color:var(--text)}',
      '.cyg-a-btn.danger{background:var(--red);border-color:var(--red);color:#fff}',
      '.cyg-a-btn.warn{background:var(--amber);border-color:var(--amber);color:#fff}',
      '.cyg-a-sp{flex:1}',
      '.cyg-a-chips{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px}',
      '.cyg-a-chip{border:1px solid var(--border2);background:var(--bg2);color:var(--text2);border-radius:999px;padding:3px 10px;font-size:12px;cursor:pointer;font-family:inherit}',
      '.cyg-a-chip[aria-pressed="true"]{background:var(--accent-glow);border-color:transparent;color:var(--accent);font-weight:600}',
      '.cyg-a-tw{overflow-x:auto}',
      '.cyg-a-table{width:100%;border-collapse:collapse;font-size:13px}',
      '.cyg-a-table th{text-align:left;font-size:11px;letter-spacing:.08em;color:var(--text2);font-weight:600;background:var(--bg3);padding:10px 12px;border-bottom:1px solid var(--border);white-space:nowrap}',
      '.cyg-a-table td{padding:10px 12px;border-bottom:1px solid var(--border);vertical-align:top}',
      /* The filter row. Sticky is deliberate: the whole point of putting a
         filter in a column header is that you can see which column it
         governs, and that stops being true the moment it scrolls away. */
      '.cyg-a-table tr.cyg-a-filters th{background:var(--bg2);padding:6px 8px;border-bottom:1px solid var(--border2);position:sticky;top:0;z-index:1}',
      '.cyg-a-fsel,.cyg-a-fin{width:100%;min-width:104px;max-width:230px;height:30px;font:inherit;font-size:12px;' +
        'border:1px solid var(--border2);border-radius:6px;background:var(--bg2);color:var(--text);padding:0 7px}',
      '.cyg-a-fsel:focus-visible,.cyg-a-fin:focus-visible{outline:2px solid var(--accent);outline-offset:1px}',
      /* A set filter is tinted, so a table that looks empty because of one is
         visibly different from a table that is empty. */
      '.cyg-a-fsel.on,.cyg-a-fin.on{border-color:var(--accent);background:var(--accent-glow);font-weight:600}',
      '.cyg-a-fsel:disabled{opacity:.5;cursor:not-allowed}',
      '.cyg-a-table tr.ev{cursor:pointer}',
      '.cyg-a-table tr.ev:hover{background:var(--hover-tint)}',
      '.cyg-a-table tr.ev.sel{background:var(--accent-glow)}',
      '.cyg-a-table tr.ev:focus-visible{outline:2px solid var(--accent);outline-offset:-2px}',
      '.cyg-a-when{font-family:var(--mono);font-size:12.5px;white-space:nowrap}',
      '.cyg-a-when small{display:block;color:var(--text3);font-family:var(--sans);font-size:11px}',
      '.cyg-a-who{display:flex;gap:9px;align-items:flex-start;min-width:190px}',
      '.cyg-a-av{width:26px;height:26px;border-radius:7px;display:grid;place-items:center;font-size:11px;font-weight:600;color:#fff;flex:none}',
      '.cyg-a-who small{display:block;color:var(--text3);font-size:11.5px}',
      '.cyg-a-act{font-family:var(--mono);font-size:12.5px;color:var(--text)}',
      '.cyg-a-what{min-width:240px}',
      '.cyg-a-what .s{color:var(--text2);font-size:12.5px;margin-top:2px}',
      '.cyg-a-tgt{font-size:12.5px;color:var(--text2);min-width:140px}',
      '.cyg-a-env{font-family:var(--mono);font-size:12px;font-weight:500}',
      '.cyg-a-env.PROD{color:var(--red)}.cyg-a-env.STAGING{color:var(--amber)}',
      '.cyg-a-env.TEST{color:var(--teal)}.cyg-a-env.DEV{color:var(--text3)}',
      '.cyg-a-ai{font-size:10.5px;font-weight:600;color:var(--purple);background:var(--purple-bg);border-radius:4px;padding:0 5px;margin-left:4px;white-space:nowrap}',
      '.cyg-a-src{font-size:10.5px;color:var(--text3);border:1px solid var(--border2);border-radius:4px;padding:0 4px;margin-left:4px}',
      '.cyg-a-table tr.gap td{background:var(--amber-bg);color:var(--amber);font-size:12.5px;text-align:center;padding:8px;border-top:1px dashed var(--amber);border-bottom:1px dashed var(--amber)}',
      '.cyg-a-table tr.gap.off td{background:var(--red-bg);color:var(--red);border-color:var(--red)}',
      '.cyg-a-foot{display:flex;justify-content:space-between;align-items:center;padding:12px 14px;color:var(--text2);font-size:12.5px;flex-wrap:wrap;gap:8px}',
      '.cyg-a-empty{padding:40px;text-align:center;color:var(--text2)}',
      '.cyg-a-scrim{position:fixed;inset:0;background:var(--modal-scrim);opacity:0;pointer-events:none;transition:opacity .2s;z-index:1400}',
      '.cyg-a-scrim.on{opacity:1;pointer-events:auto}',
      '.cyg-a-drawer{position:fixed;top:0;right:0;height:100vh;width:min(520px,100vw);background:var(--bg2);border-left:1px solid var(--border);box-shadow:var(--shadow-strong);transform:translateX(105%);transition:transform .22s ease;display:flex;flex-direction:column;z-index:1401}',
      '.cyg-a-drawer.on{transform:none}',
      '@media (prefers-reduced-motion:reduce){.cyg-a-drawer{transition:none}}',
      '.cyg-a-dh{padding:18px 20px;border-bottom:1px solid var(--border);display:flex;gap:12px;align-items:flex-start}',
      '.cyg-a-dh h2{margin:0;font-size:16px;font-family:var(--mono);font-weight:500;word-break:break-all}',
      '.cyg-a-dh p{margin:4px 0 0;color:var(--text2);font-size:13px}',
      '.cyg-a-x{margin-left:auto;border:0;background:none;font-size:20px;cursor:pointer;color:var(--text2);line-height:1;padding:0 4px}',
      '.cyg-a-db{padding:16px 20px;overflow:auto;flex:1}',
      '.cyg-a-kv{display:grid;grid-template-columns:130px 1fr;gap:6px 12px;font-size:13px;margin-bottom:18px}',
      '.cyg-a-kv dt{color:var(--text2)}.cyg-a-kv dd{margin:0;word-break:break-word}',
      '.cyg-a-db h4{font-size:11px;letter-spacing:.1em;color:var(--text2);margin:0 0 8px;font-weight:600}',
      '.cyg-a-diff{border:1px solid var(--border);border-radius:var(--r);overflow:hidden;margin-bottom:18px;font-family:var(--mono);font-size:12px}',
      '.cyg-a-diff .row{display:grid;grid-template-columns:130px 1fr 1fr;border-bottom:1px solid var(--border)}',
      '.cyg-a-diff .row:last-child{border:0}',
      '.cyg-a-diff .row>div{padding:7px 10px;word-break:break-word}',
      '.cyg-a-diff .hd{background:var(--bg3);font-family:var(--sans);font-size:11px;color:var(--text2);font-weight:600;letter-spacing:.06em}',
      '.cyg-a-diff .b{background:var(--red-bg);color:var(--red)}',
      '.cyg-a-diff .a{background:var(--green-bg);color:var(--green)}',
      '.cyg-a-hash{font-family:var(--mono);font-size:11.5px;background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:10px;color:var(--text2);word-break:break-all;line-height:1.7}',
      '.cyg-a-hash b{color:var(--text);font-weight:500}',
      '.cyg-a-modal{position:fixed;inset:0;display:none;place-items:center;background:var(--modal-scrim);z-index:1500;padding:16px}',
      '.cyg-a-modal.on{display:grid}',
      '.cyg-a-mbox{background:var(--bg2);border-radius:var(--r-lg);width:min(460px,100%);padding:20px;box-shadow:var(--shadow-strong)}',
      '.cyg-a-mbox h3{margin:0 0 6px;font-size:17px}',
      '.cyg-a-mbox p{color:var(--text2);margin:0 0 14px;font-size:13.5px}',
      '.cyg-a-mbox label{display:block;font-size:12.5px;font-weight:600;margin:10px 0 5px}',
      '.cyg-a-mbox textarea,.cyg-a-mbox select,.cyg-a-mbox input[type=text]{width:100%;border:1px solid var(--border2);border-radius:8px;padding:8px 10px;background:var(--bg2);color:var(--text);font:inherit;font-size:13px}',
      '.cyg-a-mbox textarea{min-height:70px;resize:vertical}',
      '.cyg-a-mact{display:flex;justify-content:flex-end;gap:8px;margin-top:16px}',
      '.cyg-a-note{font-size:12.5px;background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:9px 11px;color:var(--text2);margin-top:10px}',
      '.cyg-a-grid2{display:grid;grid-template-columns:1fr 1fr;gap:16px;align-items:start}',
      '.cyg-a-pad{padding:18px 20px}',
      '.cyg-a-pad h3{margin:0 0 4px;font-size:15px}',
      '.cyg-a-pad>p{margin:0 0 14px;color:var(--text2);font-size:13px}',
      '.cyg-a-cat{display:flex;align-items:center;gap:12px;padding:10px 0;border-top:1px solid var(--border)}',
      '.cyg-a-cat:first-of-type{border-top:0}',
      '.cyg-a-cat .t{flex:1}.cyg-a-cat .t b{font-weight:500;display:block}',
      '.cyg-a-cat .t small{color:var(--text3);font-size:12px}',
      '.cyg-a-tg{position:relative;width:36px;height:20px;flex:none;display:inline-block}',
      '.cyg-a-tg input{position:absolute;opacity:0;width:36px;height:20px;margin:0;cursor:pointer}',
      '.cyg-a-tg span{position:absolute;inset:0;border-radius:99px;background:var(--bg4);transition:background .15s;pointer-events:none}',
      '.cyg-a-tg span:before{content:"";position:absolute;width:16px;height:16px;border-radius:50%;background:var(--bg2);left:2px;top:2px;transition:transform .15s;box-shadow:0 1px 2px rgba(0,0,0,.25)}',
      '.cyg-a-tg input:checked+span{background:var(--accent)}',
      '.cyg-a-tg input:checked+span:before{transform:translateX(16px)}',
      '.cyg-a-tg input:disabled{cursor:not-allowed}',
      '.cyg-a-tg input:disabled+span{opacity:.55}',
      '.cyg-a-tg input:focus-visible+span{outline:2px solid var(--accent);outline-offset:2px}',
      '.cyg-a-radio{display:flex;gap:8px;flex-wrap:wrap}',
      '.cyg-a-radio label{border:1px solid var(--border2);border-radius:8px;padding:7px 12px;cursor:pointer;font-size:13px}',
      '.cyg-a-radio input{margin-right:6px}',
      '.cyg-a-chain{display:flex;gap:6px;align-items:center;overflow-x:auto;padding:6px 0 14px}',
      '.cyg-a-blk{flex:none;border:1px solid var(--green);border-radius:8px;padding:8px 10px;font-family:var(--mono);font-size:11px;background:var(--bg2);min-width:120px}',
      '.cyg-a-blk b{display:block;color:var(--text);font-size:11.5px}',
      '.cyg-a-arrow{color:var(--text3);flex:none}',
      '.cyg-a-denied{max-width:520px;margin:60px auto;text-align:center;padding:36px}',
      '.cyg-a-denied .ic-wrap{width:52px;height:52px;border-radius:14px;background:var(--red-bg);color:var(--red);display:grid;place-items:center;margin:0 auto 14px}',
      '.cyg-a-toast{position:fixed;bottom:22px;left:50%;transform:translateX(-50%) translateY(20px);background:var(--text);color:var(--bg2);padding:10px 16px;border-radius:10px;font-size:13px;opacity:0;transition:.2s;z-index:1600;pointer-events:none;max-width:90vw;text-align:center}',
      '.cyg-a-toast.on{opacity:1;transform:translateX(-50%)}',
      '.cyg-a-banner{border:1px solid var(--amber);background:var(--amber-bg);color:var(--text);border-radius:var(--r);padding:10px 13px;font-size:13px;margin-bottom:14px}',
      '@media (max-width:1000px){.cyg-a-kpis{grid-template-columns:repeat(2,minmax(0,1fr))}.cyg-a-grid2{grid-template-columns:1fr}}',
      '@media (max-width:640px){.cyg-a-status{grid-template-columns:1fr}.cyg-a-in{max-width:none;width:100%}.cyg-a-kv{grid-template-columns:1fr}.cyg-a-diff .row{grid-template-columns:90px 1fr 1fr}}',
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
      bannerHtml() + statusHtml() + kpiHtml() + tabsHtml() +
      '<div id="cyg-a-panel-events" role="tabpanel" aria-labelledby="cyg-a-tab-events"' +
        (state.tab === 'events' ? '' : ' hidden') + '></div>' +
      '<div id="cyg-a-panel-settings" role="tabpanel" aria-labelledby="cyg-a-tab-settings"' +
        (state.tab === 'settings' ? '' : ' hidden') + '></div>' +
      '<div id="cyg-a-panel-integrity" role="tabpanel" aria-labelledby="cyg-a-tab-integrity"' +
        (state.tab === 'integrity' ? '' : ' hidden') + '></div>';
    renderEventsPanel();
    renderSettingsPanel();
    renderIntegrityPanel();
    wire();
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

    return '<div class="cyg-a-card cyg-a-status' + (s === 'recording' ? '' : ' ' + s) + '">' +
      '<div><h3>' + pill + '<span>' + esc(title) + '</span></h3><p>' + text + '</p>' +
      '<div class="cyg-a-always"><i class="ic ic-shield ic-sm"></i><span>Security events are ' +
      '<b>always recorded</b>, whatever the state: sign-ins, users and roles, PROD writes, ' +
      'and every change to this switch itself.</span></div></div>' +
      seg + '</div>';
  }

  function kpiHtml() {
    var s = state.status.stats;
    // Self-scoped readers get no organisation KPIs, because a count of
    // "people making changes" computed over one person's own slice would be
    // a true number that answers a different question.
    if (!s) {
      return '<div class="cyg-a-card cyg-a-pad" style="margin-bottom:16px">' +
        '<p style="margin:0;color:var(--text2);font-size:13px">You are seeing your own entries. ' +
        'Organisation-wide counts need the Owner, Platform Administrator or Auditor role.</p></div>';
    }
    var tile = function (label, value, detail, colour) {
      return '<div class="cyg-a-card cyg-a-kpi"><div class="l">' + esc(label) + '</div>' +
        '<div class="v"' + (colour ? ' style="color:' + colour + '"' : '') + '>' + value + '</div>' +
        '<div class="d">' + esc(detail) + '</div></div>';
    };
    return '<div class="cyg-a-kpis">' +
      tile('Events · last 24h', s.events, 'across all projects') +
      tile('People making changes', s.actors, 'distinct actors, 24h') +
      tile('PROD changes', s.prodChanges, 'writes and config on PROD', s.prodChanges ? 'var(--red)' : '') +
      tile('Denied / failed', s.deniedOrFailed, 'blocked by role, or errored',
           s.deniedOrFailed ? 'var(--amber)' : '') +
      '</div>';
  }

  var TABS = [
    { key: 'events', label: 'Events', hint: 'who · what · when' },
    { key: 'settings', label: 'Capture settings', hint: 'what gets recorded' },
    { key: 'integrity', label: 'Integrity', hint: 'verify the chain' },
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
          esc(t.label) + '<small>' + esc(t.hint) + '</small></button>';
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
    var toolbar = '<div class="cyg-a-toolbar">' +
      '<input class="cyg-a-in" id="cyg-a-q" type="search" placeholder="Search everything…" ' +
        'aria-label="Search all columns" value="' + esc(f.q) + '">' +
      (activeFilterCount()
        ? '<button class="cyg-a-btn" id="cyg-a-clear" type="button">Clear ' +
          activeFilterCount() + ' filter' + (activeFilterCount() === 1 ? '' : 's') + '</button>'
        : '') +
      '<span class="cyg-a-sp"></span>' +
      (st.canExport
        ? '<button class="cyg-a-btn primary" id="cyg-a-csv" type="button">' +
          '<i class="ic ic-download ic-sm"></i>Export CSV</button>' +
          '<button class="cyg-a-btn" id="cyg-a-json" type="button">' +
          '<i class="ic ic-download ic-sm"></i>JSON</button>'
        : '<span style="font-size:12.5px;color:var(--text3)">Export needs the Platform ' +
          'Administrator or Auditor role</span>') +
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
      '<thead><tr><th>WHEN</th><th>WHO</th><th>WHAT</th><th>TARGET</th><th>ENV</th><th>OUTCOME</th></tr>' +
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
      '</tr>';
  }

  // Gap rows are interleaved by timestamp rather than appended, so a pause
  // appears in the timeline where it happened. They are only drawn on an
  // unfiltered-by-category view: a gap row inside a list filtered to
  // "connections" would be claiming something about connections that it is
  // not saying.
  function buildRows() {
    if (!state.events.length) {
      return '<tr><td colspan="6" class="cyg-a-empty">' +
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
    return '<tr class="gap' + (g.kind === 'off' ? ' off' : '') + '"><td colspan="6">' +
      what + ' ' + when + (g.by ? ' by ' + esc(g.by) : '') +
      (g.reason ? ' — "' + esc(g.reason) + '"' : '') +
      '. Security, access and PROD events in this window are still recorded and still shown.' +
      '</td></tr>';
  }

  function eventRow(e) {
    var name = e.actorName || e.actorEmail || 'System';
    var roles = (e.effectiveRoles || []).join(' ');
    return '<tr class="ev" data-seq="' + e.seq + '" tabindex="0">' +
      '<td class="cyg-a-when">' + esc(fmt(e.occurredAt)) + '<small>' + esc(rel(e.occurredAt)) + '</small></td>' +
      '<td><div class="cyg-a-who"><div class="cyg-a-av" style="background:' + avatarColour(e.actorEmail) + '">' +
        esc(initials(e.actorName, e.actorEmail)) + '</div><div>' + esc(name) +
        (e.actorType === 'assistant' ? '<span class="cyg-a-ai">via Ask Cygenix</span>' : '') +
        (e.source === 'client' ? '<span class="cyg-a-src">client</span>' : '') +
        '<small>' + esc(roles || e.actorType) +
        (e.context && e.context.ip ? ' · ' + esc(e.context.ip) : '') + '</small></div></div></td>' +
      '<td class="cyg-a-what"><div class="cyg-a-act">' + esc(e.action) + '</div>' +
        '<div class="s">' + esc(e.summary || '') + '</div></td>' +
      '<td class="cyg-a-tgt">' + esc((e.target && e.target.label) || e.resourceId || '—') + '</td>' +
      '<td><span class="cyg-a-env ' + esc(e.environment || '') + '">' + esc(e.environment || '—') + '</span></td>' +
      '<td><span class="cyg-a-pill ' + outcomeClass(e.outcome) + '">' + esc(e.outcome) + '</span></td></tr>';
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
      '<dt>Outcome</dt><dd><span class="cyg-a-pill ' + outcomeClass(e.outcome) + '">' + esc(e.outcome) +
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
      'Verify the whole chain on the Integrity tab</span></div>';

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

  function renderSettingsPanel() {
    var el = document.getElementById('cyg-a-panel-settings');
    if (!el) return;
    var st = state.status;
    var cfg = st.settings;
    var can = st.canConfigure;

    var cats = (st.categories || []).map(function (c) {
      var on = cfg.categories[c.key] !== false;
      return '<div class="cyg-a-cat"><div class="t"><b>' + esc(c.label) +
        (c.alwaysOn ? ' <span class="cyg-a-pill cyg-a-p-grey"><i class="ic ic-lock ic-sm"></i>always on</span>' : '') +
        '</b><small>' + esc(c.detail || '') + '</small></div>' +
        '<label class="cyg-a-tg"><input type="checkbox" data-cat-key="' + esc(c.key) + '"' +
        (on ? ' checked' : '') + ((c.alwaysOn || !can) ? ' disabled' : '') +
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
      return '<div class="cyg-a-cat"><div class="t"><b>' + esc(label) + '</b><small>' + esc(detail) +
        '</small></div><label class="cyg-a-tg"><input type="checkbox" data-flag="' + key + '"' +
        (cfg[key] !== false ? ' checked' : '') + (can ? '' : ' disabled') +
        ' aria-label="' + esc(label) + '"><span></span></label></div>';
    };

    el.innerHTML =
      (can ? '' : '<div class="cyg-a-banner">You can see how capture is configured but not change it. ' +
        'Changing it belongs to the Organisation Owner and the Platform Administrator — an auditor who ' +
        'can quieten the trail they report on is not an auditor.</div>') +
      '<div class="cyg-a-grid2"><div class="cyg-a-card cyg-a-pad">' +
      '<h3>What gets recorded</h3><p>Turn categories off to reduce noise. Locked rows cannot be ' +
      'disabled here or through the API — they are what a pause is not allowed to hide.</p>' +
      '<div id="cyg-a-cats">' + cats + '</div></div>' +
      '<div style="display:flex;flex-direction:column;gap:16px">' +
      '<div class="cyg-a-card cyg-a-pad"><h3>Retention</h3>' +
      '<p>Older events are archived and then purged. A purge writes its own entry and leaves a signed ' +
      'checkpoint, so verification still works across the boundary.</p>' +
      '<div class="cyg-a-radio" id="cyg-a-ret">' + retention + '</div>' +
      '<div class="cyg-a-note">The purge job is not running yet. Nothing is being deleted, whatever this ' +
      'is set to — the setting is stored and will apply when it lands.</div></div>' +
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

    el.innerHTML = '<div class="cyg-a-card cyg-a-pad">' +
      '<h3><i class="ic ic-chain ic-sm"></i> Hash chain</h3>' +
      '<p>Every entry stores the SHA-256 of the one before it, so editing or removing any past row ' +
      'breaks every hash after it. Verification walks the chain and names the first entry that does ' +
      'not line up.</p>' +
      '<div class="cyg-a-chain">' + blocks + '</div>' +
      '<div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">' +
      (state.status.canVerify
        ? '<button class="cyg-a-btn primary" id="cyg-a-verify" type="button"' +
          (state.verifying ? ' disabled' : '') + '>Verify the full chain</button>'
        : '') +
      '<span id="cyg-a-verifyout" style="font-size:13px;color:var(--text2)">' + out + '</span></div>' +
      '<div class="cyg-a-note" style="margin-top:14px"><b>The known limit, stated rather than buried.</b> ' +
      'Netlify Blobs has no transactions, so two appends at the same instant can race for the head ' +
      'position. A short retry closes the realistic window at this product\'s traffic, and verification ' +
      'reports a break rather than hiding one — but this is tamper-evident, not tamper-proof, and an ' +
      'evidence pack should say so.</div></div>';

    var vb = document.getElementById('cyg-a-verify');
    if (vb) vb.addEventListener('click', runVerify);
  }

  function runVerify() {
    state.verifying = true;
    renderIntegrityPanel();
    api('what=verify').then(function (d) {
      state.verifyResult = d;
      toast(d.ok ? 'Chain intact — ' + d.count + ' entries' : 'Chain BROKEN at entry ' + d.brokenAt);
    }).catch(function (e) {
      state.verifyResult = { ok: false, brokenAt: '?', reason: e.message };
      toast('Verification failed: ' + e.message);
    }).then(function () {
      state.verifying = false;
      renderIntegrityPanel();
    });
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
      '<label for="cyg-a-why">Reason <span style="color:var(--red)">*</span></label>' +
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
      '<label for="cyg-a-why">Reason <span style="color:var(--red)">*</span></label>' +
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
        document.getElementById('cyg-a-why').style.borderColor = 'var(--red)';
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
          document.getElementById('cyg-a-conf').style.borderColor = 'var(--red)';
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

  function wire() {
    wireStatus();
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

    var csv = document.getElementById('cyg-a-csv');
    if (csv) csv.addEventListener('click', function () { exportAs('csv'); });
    var json = document.getElementById('cyg-a-json');
    if (json) json.addEventListener('click', function () { exportAs('json'); });
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
