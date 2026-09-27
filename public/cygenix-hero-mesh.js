/* ============================================================================
   cygenix-hero-mesh.js — mounts the polygon mesh behind a page.
   ----------------------------------------------------------------------------
   The engine (cygenix-mesh.js) draws; this decides where and with what. It
   started as an inline script in index.html. When the pricing page took the
   landing page's theme it needed the same mount with the same values, and
   two copies of a tuning block are two copies that drift — so the mount is
   one file, loaded by every page that has a #cx-mesh canvas.

   It runs after DOMContentLoaded (deferred scripts, the engine included,
   have all executed by then) and is a no-op wherever #cx-mesh is absent, so
   loading it on a page without a mesh costs one lookup.

   THE VALUES
   Tuned live against the running landing page through
   cygenixHeroMesh.update() and confirmed by eye, not guessed: density 1.45,
   speed 1.0, reach 200, glow 0.80, lineAlpha 0.50, nodeAlpha 0.95. See the
   INTENSITY note in cygenix-mesh.js.

   THE COLOURS
   Read from the page's own tokens at mount, so a page that changes them
   changes the mesh: nodes --accent-ink2, connectors --accent-ink (the brand
   accent, the engine's default, is darker than the nodes and made the lines
   read as background texture rather than structure), bloom --accent. The
   fallbacks are the same three house values, so no page carries a colour
   its palette test does not already sanction.

   TWO KINDS OF LAYER (Sep-2026)
   · Hero-only (pricing): the layer sits inside .hero-stage and is sized to
     it. Behaviour exactly as it always was.
   · Page-wide (the homepage): the layer is a fixed, full-screen sibling of
     .brand-grid, marked data-scroll-fade. It is the size of the SCREEN, not
     the page — a 14,700px canvas would cost far more to draw than anything
     it could show — and it fades out as the page scrolls. See SCROLL FADE.
   The attribute is the switch, so pricing cannot pick up the fade by
   accident.

   SCROLL FADE
   At the top of the page the mesh is at full strength, as it always was.
   After half a screen of scrolling it starts to ease out, on a smoothstep
   curve (slow to start, slow to finish, no visible kink at either end), and
   it reaches zero exactly when the top of the footer comes into view.

   The bloom — the blue glow the engine paints behind the headline — fades
   faster, over the first screen alone. The layer no longer scrolls, so a
   bloom left at full strength would sit at the same spot on the screen
   behind the middle of every section, which is not what it was ever for.

   The scroll listener is passive and does nothing but ask for one animation
   frame, so a burst of scroll events costs one calculation per frame at
   most. Opacity is written to the layer, not the canvas, which the browser
   composites without repainting anything.

   When the opacity reaches zero the engine is HELD — it stops drawing and
   keeps every node where it was (hold() in cygenix-mesh.js) — so the long
   middle and bottom of the page cost no CPU for a mesh nobody can see.
   Scrolling back up releases it, and it resumes from the same field rather
   than a reshuffled one.

   The fade's end point is measured from the footer, and re-measured when
   the window resizes, when the page's height changes (images and fonts
   arriving, a section expanding), and once more on load.

   REDUCED MOTION
   On the page-wide layer, someone who has asked their device for less
   motion gets no mesh at all: index.html hides the layer with a media query
   and this file does not start the engine, so it costs nothing. The page's
   fixed grid still gives it texture. This is stricter than the hero-only
   layer, which degrades to a slow drift (see REDUCED MOTION in
   cygenix-mesh.js): a slow drift confined to the hero is one thing, a
   moving field behind every paragraph of a 14,700px page is another.
   cygenix-a11y.js offers text size and contrast and has no motion setting,
   so the device preference is the only signal there is. If it changes while
   the page is open, this follows it.

   window.cygenixHeroMesh keeps the handle, so the console can tune live:
   cygenixHeroMesh.update({ speed: 0.3, reach: 220 }).
   ========================================================================== */
document.addEventListener('DOMContentLoaded', function () {
  var el = document.getElementById('cx-mesh');
  if (!el || !window.CygenixMesh) return;
  var layer = el.parentElement;
  var pageWide = !!(layer && layer.hasAttribute('data-scroll-fade'));

  var tok = function (name, fallback) {
    var v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return /^#[0-9a-f]{3,6}$/i.test(v) ? v : fallback;
  };
  var GLOW = 0.80;
  var OPTS = {
    density:   1.45,   // denser field — more polygon closure
    speed:     1.0,    // tuned live: below ~0.5 it did not read as motion
    reach:     200,    // longer connections so triangles actually form
    glow:      GLOW,   // blue bloom behind the headline
    parallax:  0.55,   // layer shift against the pointer and the camera sweep
    lineAlpha: 0.50,   // link opacity, tuned live with the brighter connector colour
    nodeAlpha: 0.95,
    node:      tok('--accent-ink2', '#a9b6ff'),
    line:      tok('--accent-ink', '#8ea0ff'),
    glowColor: tok('--accent', '#4a5bd6')
  };

  // Hero-only: exactly as before.
  if (!pageWide) {
    window.cygenixHeroMesh = CygenixMesh.mount(el, OPTS);
    return;
  }

  /* ── Page-wide ─────────────────────────────────────────────────────── */
  var mq = window.matchMedia ? window.matchMedia('(prefers-reduced-motion: reduce)') : null;
  var mesh = null;

  // Where the fade ends: the scroll position at which the top of the
  // footer reaches the bottom of the screen. Without a footer, the bottom
  // of the page. Recomputed by measure(), read by paint().
  var fadeStart = 0, fadeEnd = 1, vh = 1;
  function measure() {
    vh = window.innerHeight || document.documentElement.clientHeight || 1;
    var foot = document.querySelector('footer');
    var docH = document.documentElement.scrollHeight;
    var end = foot
      ? foot.getBoundingClientRect().top + window.pageYOffset - vh
      : docH - vh;
    fadeStart = vh * 0.5;
    // A page shorter than a screen and a half still fades rather than
    // dividing by zero or snapping off.
    fadeEnd = Math.max(fadeStart + 1, end);
  }

  function smooth(p) { p = p < 0 ? 0 : p > 1 ? 1 : p; return p * p * (3 - 2 * p); }

  var lastOpacity = -1, lastGlow = -1;
  function paint() {
    ticking = false;
    if (!mesh) return;
    // Reduced motion switched on mid-visit: the layer is hidden by CSS, and
    // a scroll must not wake an engine nobody can see.
    if (mq && mq.matches) { if (!mesh.isHeld()) mesh.hold(); return; }
    var y = window.pageYOffset || document.documentElement.scrollTop || 0;
    var opacity = 1 - smooth((y - fadeStart) / (fadeEnd - fadeStart));
    var glow = GLOW * (1 - smooth(y / vh));
    // Snap the last invisible sliver to zero, so the hold below is reached
    // rather than approached forever.
    if (opacity < 0.005) opacity = 0;

    if (Math.abs(opacity - lastOpacity) > 0.001) {
      layer.style.opacity = opacity === 1 ? '' : opacity.toFixed(3);
      lastOpacity = opacity;
    }
    if (Math.abs(glow - lastGlow) > 0.004) {
      mesh.tune({ glow: glow });
      lastGlow = glow;
    }
    if (opacity === 0) { if (!mesh.isHeld()) mesh.hold(); }
    else if (mesh.isHeld()) mesh.release();
  }

  var ticking = false;
  function request() {
    if (ticking) return;
    ticking = true;
    window.requestAnimationFrame(paint);
  }
  function remeasure() { measure(); request(); }

  function startMesh() {
    if (mesh) { mesh.release(); remeasure(); return; }
    mesh = CygenixMesh.mount(el, OPTS);
    window.cygenixHeroMesh = mesh;
    remeasure();
  }

  // Listeners go on once, whatever the motion preference, so a preference
  // switched off mid-visit has a working fade to come back to.
  window.addEventListener('scroll', request, { passive: true });
  window.addEventListener('resize', remeasure, { passive: true });
  window.addEventListener('load', remeasure);
  if ('ResizeObserver' in window) new ResizeObserver(remeasure).observe(document.body);

  if (mq && mq.matches) {
    // No mesh at all: index.html hides the layer, and nothing is mounted,
    // so nothing draws.
  } else {
    startMesh();
  }

  if (mq) {
    var onPref = function () {
      if (mq.matches) { if (mesh) mesh.hold(); }
      else startMesh();
    };
    if (mq.addEventListener) mq.addEventListener('change', onPref);
    else if (mq.addListener) mq.addListener(onPref);
  }
});
