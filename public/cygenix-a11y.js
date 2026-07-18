/* cygenix-a11y.js — lightweight accessibility controls.
 *
 * Injects a floating "Accessibility" button that opens a small panel with:
 *   • Text size  — A− / A+ scales the whole page (100%–145%). Uses CSS `zoom`
 *                  so it works even though the page sizes text in px.
 *   • High contrast — boosts text/border contrast by re-mapping the theme's
 *                  CSS variables (pure black text on white, darker accent,
 *                  underlined links).
 *
 * Choices persist in localStorage ('cygenix_a11y') and re-apply on load.
 * Self-contained: drop <script src="/cygenix-a11y.js"></script> before </body>
 * on any page. Nothing else required.
 */
(function () {
  'use strict';
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

  // ── Styles ────────────────────────────────────────────────────────────────
  var css = [
    /* High-contrast palette — re-maps the theme variables the page already uses. */
    'html.a11y-hc{',
    '  --bg:#ffffff;--bg2:#ffffff;--bg3:#efefef;--bg4:#e2e2e2;',
    '  --text:#000000;--text2:#161616;--text3:#2e2e2e;',
    '  --border:rgba(0,0,0,0.45);--border2:rgba(0,0,0,0.72);',
    '  --accent:#9e2a1b;--accent2:#7d2015;--accent-glow:rgba(158,42,27,0.18);',
    '  --glow:rgba(158,42,27,0.14);',
    '}',
    'html.a11y-hc a{text-decoration:underline}',
    'html.a11y-hc .nav-cta,html.a11y-hc .btn-primary,html.a11y-hc .nav-links a{text-decoration:none}',
    'html.a11y-hc .nav-links a:hover,html.a11y-hc a.footer-link{text-decoration:underline}',

    /* Floating control */
    '#a11y-fab{position:fixed;left:1.25rem;bottom:1.25rem;z-index:10050;width:46px;height:46px;border-radius:50%;',
    '  background:var(--accent,#C74634);color:#fff;border:2px solid rgba(255,255,255,0.85);cursor:pointer;',
    '  box-shadow:0 4px 16px rgba(0,0,0,0.28);display:flex;align-items:center;justify-content:center;padding:0}',
    '#a11y-fab:focus-visible{outline:3px solid #1a73e8;outline-offset:2px}',
    '#a11y-fab svg{width:24px;height:24px}',
    '#a11y-panel{position:fixed;left:1.25rem;bottom:5rem;z-index:10050;width:236px;background:var(--bg2,#fff);',
    '  color:var(--text,#111);border:1px solid var(--border2,rgba(0,0,0,0.2));border-radius:12px;',
    '  box-shadow:0 12px 40px rgba(0,0,0,0.22);padding:0.85rem;font-family:var(--sans,system-ui,sans-serif)}',
    '#a11y-panel[hidden]{display:none}',
    '#a11y-panel .a11y-h{font-size:13px;font-weight:700;color:var(--text,#111);margin-bottom:0.7rem;display:flex;align-items:center;gap:0.4rem}',
    '#a11y-panel .a11y-h svg{width:17px;height:17px;flex-shrink:0;color:var(--accent,#C74634)}',
    '#a11y-panel .a11y-row{display:flex;align-items:center;justify-content:space-between;gap:0.6rem;margin-bottom:0.7rem}',
    '#a11y-panel .a11y-row > span{font-size:12.5px;color:var(--text2,#555)}',
    '#a11y-panel .a11y-size{display:flex;align-items:center;gap:0.35rem}',
    '#a11y-panel .a11y-size button{width:30px;height:28px;border:1px solid var(--border2,rgba(0,0,0,0.2));background:var(--bg,#f6f6f6);',
    '  color:var(--text,#111);border-radius:7px;cursor:pointer;font-size:13px;font-weight:700;line-height:1}',
    '#a11y-panel .a11y-size button:hover{border-color:var(--accent,#C74634);color:var(--accent,#C74634)}',
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
    '#a11y-panel .a11y-reset:hover{color:var(--accent,#C74634);border-color:var(--accent,#C74634)}',
    /* Keep the control itself an even, comfortable size regardless of page zoom. */
    '#a11y-fab,#a11y-panel{zoom:1}'
  ].join('\n');
  var st = document.createElement('style');
  st.id = 'a11y-style';
  st.textContent = css;
  document.head.appendChild(st);

  // ── Markup ────────────────────────────────────────────────────────────────
  var A11Y_ICON = '<svg viewBox="0 0 24 24" fill="none" aria-hidden="true">'
    + '<circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="1.6"/>'
    + '<circle cx="12" cy="6.6" r="1.3" fill="currentColor"/>'
    + '<path d="M6.5 9.2c1.8.9 3.6 1.2 5.5 1.2s3.7-.3 5.5-1.2M12 10.4V15m0 0l-2.2 3.4M12 15l2.2 3.4" '
    + 'stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/></svg>';

  var fab = document.createElement('button');
  fab.id = 'a11y-fab';
  fab.setAttribute('aria-label', 'Accessibility options');
  fab.setAttribute('aria-expanded', 'false');
  fab.setAttribute('aria-controls', 'a11y-panel');
  fab.innerHTML = A11Y_ICON;

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

  document.body.appendChild(fab);
  document.body.appendChild(panel);

  // ── Behaviour ─────────────────────────────────────────────────────────────
  function apply() {
    // Text size via zoom (works on px-sized pages). '' clears it at 100%.
    document.body.style.zoom = state.size === 1 ? '' : String(state.size);
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
  function togglePanel(open) {
    var show = open === undefined ? panel.hidden : open;
    panel.hidden = !show;
    fab.setAttribute('aria-expanded', show ? 'true' : 'false');
    if (show) { var f = document.getElementById('a11y-inc'); if (f) f.focus(); }
  }

  fab.addEventListener('click', function () { togglePanel(); });
  document.getElementById('a11y-inc').addEventListener('click', function () { step(1); });
  document.getElementById('a11y-dec').addEventListener('click', function () { step(-1); });
  document.getElementById('a11y-hc').addEventListener('click', function () { state.contrast = !state.contrast; apply(); save(); });
  document.getElementById('a11y-reset').addEventListener('click', function () { state = { size: 1, contrast: false }; apply(); save(); });

  // Close on outside click / Escape.
  document.addEventListener('click', function (e) {
    if (panel.hidden) return;
    if (!panel.contains(e.target) && e.target !== fab && !fab.contains(e.target)) togglePanel(false);
  });
  document.addEventListener('keydown', function (e) { if (e.key === 'Escape' && !panel.hidden) { togglePanel(false); fab.focus(); } });

  apply();

  // Deep link: /#accessibility opens the panel (handy for support/footer links).
  if (location.hash === '#accessibility') togglePanel(true);
})();
