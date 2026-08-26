// public/cygenix-guardrail.js
//
// The client half of the server-enforced guardrail.
//
// The server decides. When a tenant's guardrail covers an act, db-connect
// answers 428 — not refused, unfinished — and names what is missing: either
// a confirmation from the person running it, or an approval from somebody
// else. This module turns that answer into the dialog it deserves and
// re-sends the request with the approval attached.
//
// It is deliberately a wrapper around fetch rather than a new API, so a
// caller adopts it by changing one expression:
//
//     const res = await fetch(url, init);
//     const res = await CygenixGuardrail.fetch(url, init);
//
// Everything downstream — the JSON parse, the error handling, the timeout —
// stays exactly as it was. If this file is not loaded, the raw fetch still
// works and the 428 surfaces as an ordinary error with a readable message,
// which is why the callers guard on `window.CygenixGuardrail` rather than
// assuming it.
//
// What this module CANNOT do is bypass anything. The approval id it sends
// back was issued by the server against a hash of the exact statement; a
// client that fabricates one, replays a spent one, or edits the SQL after
// approval gets the same 428 again.

(function (global) {
  'use strict';

  var ADMIN = '/.netlify/functions/rbac-admin';

  // ── The dialog ───────────────────────────────────────────────────────────
  //
  // Blast radius, not "are you sure". The operator is told what will run,
  // where, and what the server's own destructive-statement analysis found.

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  function destructiveList(destructive) {
    if (!destructive || !destructive.length) return '';
    var rows = destructive.map(function (d) {
      return '<li><strong>' + esc(d.type) + '</strong> on <code>' + esc(d.table) + '</code>' +
             (d.bounded ? ' <span class="cg-ok">(has a WHERE clause)</span>'
                        : ' <span class="cg-bad">(no WHERE clause — every row)</span>') + '</li>';
    }).join('');
    return '<ul class="cg-list">' + rows + '</ul>';
  }

  function ensureStyles() {
    if (document.getElementById('cg-style')) return;
    var st = document.createElement('style');
    st.id = 'cg-style';
    st.textContent = [
      '.cg-back{position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:10050;display:flex;',
      'align-items:center;justify-content:center;padding:1rem}',
      '.cg-box{background:var(--bg2,#161a22);color:var(--text,#e6e9ef);border:1px solid rgba(255,255,255,.14);',
      'border-radius:12px;max-width:560px;width:100%;padding:1.25rem 1.4rem;box-shadow:0 24px 64px rgba(0,0,0,.5)}',
      '.cg-box h3{margin:0 0 .5rem;font-size:1.05rem}',
      '.cg-env{display:inline-block;font-size:.7rem;letter-spacing:.06em;text-transform:uppercase;',
      'padding:.15rem .5rem;border-radius:999px;border:1px solid currentColor;margin-left:.5rem}',
      '.cg-env.prod{color:#ff8a8a}.cg-env.staging{color:#ffc46b}',
      '.cg-box p{margin:.55rem 0;font-size:.88rem;line-height:1.5;color:var(--text2,#a9b1c1)}',
      '.cg-list{margin:.5rem 0;padding-left:1.1rem;font-size:.85rem}',
      '.cg-list li{margin:.25rem 0}',
      '.cg-list code{background:rgba(255,255,255,.07);padding:.05rem .3rem;border-radius:4px}',
      '.cg-ok{color:#7fd18a}.cg-bad{color:#ff8a8a;font-weight:600}',
      '.cg-sql{background:rgba(0,0,0,.35);border:1px solid rgba(255,255,255,.1);border-radius:8px;',
      'padding:.55rem .7rem;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.78rem;',
      'max-height:120px;overflow:auto;white-space:pre-wrap;word-break:break-word;margin:.5rem 0}',
      '.cg-actions{display:flex;gap:.5rem;justify-content:flex-end;margin-top:1rem;flex-wrap:wrap}',
      '.cg-btn{padding:.5rem .9rem;border-radius:8px;border:1px solid rgba(255,255,255,.18);',
      'background:transparent;color:inherit;font:inherit;cursor:pointer}',
      '.cg-btn.primary{background:var(--accent,#3b82f6);border-color:transparent;color:#fff}',
      '.cg-btn.danger{background:#c0392b;border-color:transparent;color:#fff}',
      '.cg-btn:focus-visible{outline:2px solid var(--accent,#3b82f6);outline-offset:2px}',
      '@media (max-width:520px){.cg-actions{flex-direction:column-reverse}.cg-btn{width:100%}}',
    ].join('');
    document.head.appendChild(st);
  }

  // Resolves true (proceed) or false (cancelled). Focus is trapped to the
  // dialog's own controls and Escape cancels — this blocks a destructive act,
  // so it must not be dismissable by accident.
  function confirmDialog(info, sql) {
    ensureStyles();
    return new Promise(function (resolve) {
      var back = document.createElement('div');
      back.className = 'cg-back';
      back.setAttribute('role', 'dialog');
      back.setAttribute('aria-modal', 'true');
      back.setAttribute('aria-labelledby', 'cg-title');

      var env = String(info.environment || '').toUpperCase();
      var envCls = env === 'PROD' ? 'prod' : (env === 'STAGING' ? 'staging' : '');
      var unbounded = (info.destructive || []).some(function (d) { return !d.bounded; });

      back.innerHTML =
        '<div class="cg-box">' +
          '<h3 id="cg-title">Confirm this change' +
            (env ? '<span class="cg-env ' + envCls + '">' + esc(env) + '</span>' : '') + '</h3>' +
          '<p>Your organisation requires changes on ' + esc(env || 'this target') +
            ' to be confirmed before they run. The server issued this confirmation ' +
            'for the exact statement below — editing it means confirming again.</p>' +
          (sql ? '<div class="cg-sql">' + esc(String(sql).slice(0, 1200)) + '</div>' : '') +
          destructiveList(info.destructive) +
          (unbounded ? '<p class="cg-bad">At least one statement has no WHERE clause. ' +
                       'It will affect every row in the table.</p>' : '') +
          '<div class="cg-actions">' +
            '<button type="button" class="cg-btn" data-cg="cancel">Cancel</button>' +
            '<button type="button" class="cg-btn ' + (unbounded ? 'danger' : 'primary') + '" data-cg="go">' +
              (unbounded ? 'Run it anyway' : 'Confirm and run') + '</button>' +
          '</div>' +
        '</div>';

      var previously = document.activeElement;
      function close(result) {
        document.removeEventListener('keydown', onKey, true);
        back.remove();
        if (previously && previously.focus) { try { previously.focus(); } catch (e) {} }
        resolve(result);
      }
      function onKey(e) {
        if (e.key === 'Escape') { e.preventDefault(); close(false); return; }
        if (e.key !== 'Tab') return;
        var f = back.querySelectorAll('button');
        if (!f.length) return;
        var first = f[0], last = f[f.length - 1];
        if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
        else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
      }
      back.addEventListener('click', function (e) {
        var a = e.target.closest && e.target.closest('[data-cg]');
        if (!a) return;
        close(a.getAttribute('data-cg') === 'go');
      });
      document.addEventListener('keydown', onKey, true);
      document.body.appendChild(back);
      var go = back.querySelector('[data-cg="cancel"]');
      if (go && go.focus) go.focus();
    });
  }

  // The two-person case. Nothing to confirm — somebody else has to act — so
  // this tells the operator where their request went and stops.
  function awaitingDialog(info) {
    ensureStyles();
    return new Promise(function (resolve) {
      var back = document.createElement('div');
      back.className = 'cg-back';
      back.setAttribute('role', 'dialog');
      back.setAttribute('aria-modal', 'true');
      var waiting = (info.approvedBy || []).length > 0;
      back.innerHTML =
        '<div class="cg-box">' +
          '<h3>Sent for approval</h3>' +
          '<p>Your organisation runs a two-person rule on ' + esc(info.environment || 'this target') +
            '. This change is queued and will not run until an Approver, Migration Lead or ' +
            'Platform Administrator authorises it. You cannot approve your own request.</p>' +
          destructiveList(info.destructive) +
          '<p>Request <code>' + esc(info.approvalId || '') + '</code>' +
          (info.approvalExpiresAt ? ', expires ' + esc(new Date(info.approvalExpiresAt).toLocaleTimeString()) : '') +
          '. It is waiting on the Users &amp; Roles page' + (waiting ? '' : '') + '.</p>' +
          '<div class="cg-actions">' +
            '<a class="cg-btn" href="/user-roles#approvals">Open Users &amp; Roles</a>' +
            '<button type="button" class="cg-btn primary" data-cg="ok">Close</button>' +
          '</div>' +
        '</div>';
      function close() { back.remove(); document.removeEventListener('keydown', onKey, true); resolve(); }
      function onKey(e) { if (e.key === 'Escape') { e.preventDefault(); close(); } }
      back.addEventListener('click', function (e) {
        if (e.target.closest && e.target.closest('[data-cg="ok"]')) close();
      });
      document.addEventListener('keydown', onKey, true);
      document.body.appendChild(back);
      var b = back.querySelector('[data-cg="ok"]');
      if (b && b.focus) b.focus();
    });
  }

  // ── The wrapper ──────────────────────────────────────────────────────────

  function bodyOf(init) {
    try { return JSON.parse((init && init.body) || '{}'); } catch (e) { return null; }
  }

  // Drop-in for fetch. Only a 428 carrying `guardrailRequired` is treated
  // specially; every other status, including 403, is handed straight back so
  // existing error handling is untouched.
  async function guardedFetch(url, init) {
    var res = await fetch(url, init);
    if (res.status !== 428) return res;

    var info;
    try { info = await res.clone().json(); } catch (e) { return res; }
    if (!info || !info.guardrailRequired || !info.approvalId) return res;

    if (info.guardrailRequired === 'second-person') {
      await awaitingDialog(info);
      return res;   // the caller reports it; the act has not run
    }

    var payload = bodyOf(init);
    if (!payload) return res;
    var proceed = await confirmDialog(info, payload.sql ||
      (Array.isArray(payload.batchSql) ? payload.batchSql.join(';\n') : ''));
    if (!proceed) {
      // Withdraw the request the server opened, so a cancelled act does not
      // leave a live approval lying around for fifteen minutes.
      try {
        await fetch(ADMIN, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ op: 'withdraw-approval', approvalId: info.approvalId }),
        });
      } catch (e) { /* best effort — it expires on its own */ }
      return res;
    }

    payload.approvalId = info.approvalId;
    var retry = Object.assign({}, init, { body: JSON.stringify(payload) });
    return fetch(url, retry);
  }

  global.CygenixGuardrail = {
    fetch: guardedFetch,
    confirmDialog: confirmDialog,
    awaitingDialog: awaitingDialog,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = global.CygenixGuardrail;
  }
})(typeof window !== 'undefined' ? window : globalThis);
