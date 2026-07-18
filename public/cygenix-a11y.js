/* cygenix-a11y.js — accessibility engine (text size + high contrast).
 *
 * Injects a small settings PANEL and exposes a global API so any trigger can
 * open it — the app's sidebar footer, a homepage footer link, etc. There is no
 * floating button; callers provide their own trigger and give it the class
 * `a11y-trigger` so an outside-click doesn't immediately re-close the panel.
 *
 *   window.CygenixA11y.toggle()   // open/close the panel
 *   window.CygenixA11y.open()
 *   window.CygenixA11y.close()
 *
 * Controls:
 *   • Text size — A− / A+ scales the page 100%–145% via CSS `zoom` (works even
 *     though pages size text in px).
 *   • High contrast — re-maps the theme CSS variables to pure-black text on
 *     white, a darker accent and underlined links.
 * Choices persist in localStorage ('cygenix_a11y') and re-apply on load.
 */
(function () {
  'use strict';
  if (window.CygenixA11y) return;                 // guard against double-load

  var KEY = 'cygenix_a11y';
  var SIZES = [1, 1.15, 1.3, 1.45];
  var state = { size: 1, contrast: false };
  try {
    var saved = JSON.parse(localStorage.getItem(KEY) || '{}');
    if (saved && typeof saved === 'object') {
      if (SIZES.indexOf(saved.size) >= 0) state.size = saved.size;
      state.contrast = !!saved.contrast;
    }
  } catch (e) {}

  var A11Y_ICON = '<svg viewBox="0 0 24 24" fill="none" aria-hidden="true">'
    + '<circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="1.6"/>'
    + '<circle cx="12" cy="6.6" r="1.3" fill="currentColor"/>'
    + '<path d="M6.5 9.2c1.8.9 3.6 1.2 5.5 1.2s3.7-.3 5.5-1.2M12 10.4V15m0 0l-2.2 3.4M12 15l2.2 3.4" '
    + 'stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/></svg>';

  // ── Styles ────────────────────────────────────────────────────────────────
  var css = [
    'html.a11y-hc{',
    '  --bg:#ffffff;--bg2:#ffffff;--bg3:#efefef;--bg4:#e2e2e2;',
    '  --text:#000000;--text2:#161616;--text3:#2e2e2e;',
    '  --border:rgba(0,0,0,0.45);--border2:rgba(0,0,0,0.72);',
    '  --accent:#9e2a1b;--accent2:#7d2015;--accent-glow:rgba(158,42,27,0.18);',
    '  --glow:rgba(158,42,27,0.14);--hover-tint:rgba(0,0,0,0.06);',
    '}',
    'html.a11y-hc a{text-decoration:underline}',
    'html.a11y-hc .nav-cta,html.a11y-hc .btn-primary,html.a11y-hc .nav-links a,html.a11y-hc .cyg-nav-item,html.a11y-hc .cyg-a11y-btn{text-decoration:none}',
    'html.a11y-hc .nav-links a:hover,html.a11y-hc a.footer-link{text-decoration:underline}',

    /* Text-size zoom is applied to <html>. When active, let the window scroll so
       nothing is trapped off-screen, and cap the fixed sidebar to the visible
       viewport so its footer (the Accessibility button) stays reachable. */
    'html.a11y-zoomed{ overflow:auto !important; }',
    'html.a11y-zoomed body{ overflow:auto !important; }',
    'html.a11y-zoomed .cyg-sidebar{ height:calc(100vh / var(--a11y-zoom,1)) !important; bottom:auto !important; }',

    '#a11y-panel{position:fixed;left:12px;bottom:64px;z-index:10050;width:220px;background:var(--bg2,#fff);',
    '  color:var(--text,#111);border:1px solid var(--border2,rgba(0,0,0,0.2));border-radius:12px;',
    '  box-shadow:0 12px 40px rgba(0,0,0,0.22);padding:0.85rem;font-family:var(--sans,var(--serif,system-ui),sans-serif)}',
    '#a11y-panel[hidden]{display:none}',
    '#a11y-panel .a11y-h{font-size:13px;font-weight:700;color:var(--text,#111);margin-bottom:0.7rem;display:flex;align-items:center;gap:0.4rem}',
    '#a11y-panel .a11y-h svg{width:17px;height:17px;flex-shrink:0;color:var(--accent,#C74634)}',
    '#a11y-panel .a11y-row{display:flex;align-items:center;justify-content:space-between;gap:0.6rem;margin-bottom:0.7rem}',
    '#a11y-panel .a11y-row > span{font-size:12.5px;color:var(--text2,#555)}',
    '#a11y-panel .a11y-size{display:flex;align-items:center;gap:0.35rem}',
    '#a11y-panel .a11y-size button{width:30px;height:28px;border:1px solid var(--border2,rgba(0,0,0,0.2));background:var(--bg,#f6f6f6);',
    '  color:var(--text,#111);border-radius:7px;cursor:pointer;font-size:13px;font-weight:700;line-height:1}',
    '#a11y-panel .a11y-size button:hover{border-color:var(--accent,#C74634);color:var(--accent,#C74634)}',
    '#a11y-panel .a11y-size button:disabled{opacity:0.4;cursor:not-allowed}',
    '#a11y-panel .a11y-size span{font-size:12px;color:var(--text2,#555);min-width:38px;text-align:center;font-variant-numeric:tabular-nums}',
    '#a11y-panel .a11y-switch{width:44px;height:24px;border-radius:999px;border:1px solid var(--border2,rgba(0,0,0,0.2));',
    '  background:var(--bg4,#e2e2e2);position:relative;cursor:pointer;padding:0;transition:background .15s}',
    '#a11y-panel .a11y-switch .knob{position:absolute;top:2px;left:2px;width:18px;height:18px;border-radius:50%;background:#fff;',
    '  box-shadow:0 1px 3px rgba(0,0,0,0.3);transition:left .15s}',
    '#a11y-panel .a11y-switch.on{background:var(--accent,#C74634)}',
    '#a11y-panel .a11y-switch.on .knob{left:22px}',
    '#a11y-panel .a11y-switch:focus-visible,#a11y-panel .a11y-size button:focus-visible{outline:3px solid #1a73e8;outline-offset:2px}',
    '#a11y-panel .a11y-reset{width:100%;margin-top:0.15rem;padding:6px;font-size:12px;font-weight:600;color:var(--text2,#555);',
    '  background:none;border:1px solid var(--border,rgba(0,0,0,0.15));border-radius:7px;cursor:pointer}',
    '#a11y-panel .a11y-reset:hover{color:var(--accent,#C74634);border-color:var(--accent,#C74634)}'
  ].join('\n');
  var st = document.createElement('style');
  st.id = 'a11y-style';
  st.textContent = css;
  document.head.appendChild(st);

  // ── Panel ─────────────────────────────────────────────────────────────────
  var panel = document.createElement('div');
  panel.id = 'a11y-panel';
  panel.setAttribute('role', 'dialog');
  panel.setAttribute('aria-label', 'Accessibility options');
  panel.hidden = true;
  panel.innerHTML =
    '<div class="a11y-h">' + A11Y_ICON + 'Accessibility</div>'
    + '<div class="a11y-row"><span>Text size</span><div class="a11y-size">'
    +   '<button id="a11y-dec" aria-label="Decrease text size">A−</button>'
    +   '<span id="a11y-size-val">100%</span>'
    +   '<button id="a11y-inc" aria-label="Increase text size">A+</button>'
    + '</div></div>'
    + '<div class="a11y-row"><span>High contrast</span>'
    +   '<button id="a11y-hc" class="a11y-switch" role="switch" aria-pressed="false" aria-label="Toggle high contrast"><span class="knob"></span></button>'
    + '</div>'
    + '<button id="a11y-reset" class="a11y-reset">Reset</button>';

  function mount() {
    if (!document.body) { document.addEventListener('DOMContentLoaded', mount); return; }
    document.body.appendChild(panel);
    document.getElementById('a11y-inc').addEventListener('click', function () { step(1); });
    document.getElementById('a11y-dec').addEventListener('click', function () { step(-1); });
    document.getElementById('a11y-hc').addEventListener('click', function () { state.contrast = !state.contrast; apply(); save(); });
    document.getElementById('a11y-reset').addEventListener('click', function () { state = { size: 1, contrast: false }; apply(); save(); });
    apply();
    if (location.hash === '#accessibility') toggle(true);
  }

  // ── Behaviour ─────────────────────────────────────────────────────────────
  // Zoom the <html> root (not <body>): the root is the window's scroll
  // container, so overflow becomes scrollable instead of being clipped by an
  // app shell's `overflow:hidden`. A CSS rule keeps the fixed sidebar fitting
  // the visible viewport so its footer (Accessibility button) stays reachable.
  function applyZoom() {
    var root = document.documentElement;
    if (state.size === 1) {
      root.style.zoom = '';
      root.style.removeProperty('--a11y-zoom');
      root.classList.remove('a11y-zoomed');
    } else {
      root.style.zoom = String(state.size);
      root.style.setProperty('--a11y-zoom', String(state.size));
      root.classList.add('a11y-zoomed');
    }
  }
  function apply() {
    applyZoom();
    document.documentElement.classList.toggle('a11y-hc', state.contrast);
    var val = document.getElementById('a11y-size-val');
    if (val) val.textContent = Math.round(state.size * 100) + '%';
    var hc = document.getElementById('a11y-hc');
    if (hc) { hc.classList.toggle('on', state.contrast); hc.setAttribute('aria-pressed', state.contrast ? 'true' : 'false'); }
    var dec = document.getElementById('a11y-dec'), inc = document.getElementById('a11y-inc');
    if (dec) dec.disabled = state.size <= SIZES[0];
    if (inc) inc.disabled = state.size >= SIZES[SIZES.length - 1];
  }
  function save() { try { localStorage.setItem(KEY, JSON.stringify(state)); } catch (e) {} }
  function step(dir) {
    var i = SIZES.indexOf(state.size); if (i < 0) i = 0;
    i = Math.min(SIZES.length - 1, Math.max(0, i + dir));
    state.size = SIZES[i]; apply(); save();
  }
  function toggle(force) {
    var show = force === undefined ? panel.hidden : force;
    panel.hidden = !show;
    var triggers = document.querySelectorAll('.a11y-trigger');
    for (var i = 0; i < triggers.length; i++) triggers[i].setAttribute('aria-expanded', show ? 'true' : 'false');
    if (show) { var f = document.getElementById('a11y-inc'); if (f) f.focus(); }
  }

  document.addEventListener('click', function (e) {
    if (panel.hidden) return;
    if (panel.contains(e.target)) return;
    if (e.target.closest && e.target.closest('.a11y-trigger')) return;   // the trigger toggles itself
    toggle(false);
  });
  document.addEventListener('keydown', function (e) { if (e.key === 'Escape' && !panel.hidden) toggle(false); });

  window.CygenixA11y = { open: function () { toggle(true); }, close: function () { toggle(false); }, toggle: function () { toggle(); } };

  // Apply saved prefs immediately (zoom + contrast) even before the panel mounts.
  applyZoom();
  document.documentElement.classList.toggle('a11y-hc', state.contrast);
  mount();
})();
