/* ============================================================================
   brand-stream.js — the flowing data-stream background behind the landing
   page's hero.
   ----------------------------------------------------------------------------
   WHY IT IS A FILE OF ITS OWN
   index.html carries its own styles and its own page logic inline, and the
   shared modules it loads (cookie consent, the accessibility engine) are
   separate deferred files stamped with a content hash by
   scripts/stamp-assets.js. This is the second kind: it is a self-contained
   module with tuning knobs (window.BrandStream.apply), it is large enough
   that a cached stale copy would matter, and keeping it out of the page
   keeps tests/landing-theme.test.js — which scans index.html for colours
   outside the house palette — looking at the page's colours rather than at
   rgb triplets inside a canvas routine. Its CSS is the exception and lives
   inline in index.html next to .brand-glow and .brand-grid, because it is a
   third member of that set and belongs beside the other two.

   WHERE IT SITS
   A fixed, pointer-events:none <canvas> inserted immediately after
   .brand-glow, so it renders under .brand-grid and under all page content
   (everything else on the page is position:relative; z-index:1). It queries
   .brand-glow when it runs, so it is loaded with `defer` at the end of
   <body>: that div exists by then.

   WHAT IT MUST NEVER DO
   Cost the page a Lighthouse point, eat a click, reach a screen reader, or
   keep animating behind body copy. So: no dependency, no build step, sprites
   pre-rendered once and drawn with drawImage rather than fresh gradients per
   frame, devicePixelRatio capped at 1.5, aria-hidden, a single static frame
   under prefers-reduced-motion with no requestAnimationFrame loop at all,
   paused on a hidden tab, and faded out and stopped once the reader is more
   than ~1.15 viewport heights down the page. tests/brand-stream.test.js runs
   it against a fake DOM and pins every one of those.

   INTENSITY
   The values in CFG are the "balanced" setting. Two alternatives, for the
   day somebody wants it quieter or louder — change CFG, not the drawing:

     subtle:   laneGap 68, maxLanes 15, speedMin 30, speedMax 78,  trailMin 90,  trailMax 280, twoPacket 0.26, layer 0.42
     balanced: laneGap 52, maxLanes 22, speedMin 40, speedMax 110, trailMin 100, trailMax 340, twoPacket 0.45, layer 0.62  <- current
     bold:     laneGap 42, maxLanes 28, speedMin 52, speedMax 140, trailMin 120, trailMax 400, twoPacket 0.62, layer 0.80

   ONE DEPARTURE FROM THE ROUTINE AS SUPPLIED
   It set the layer's opacity as an inline style, always. An inline style
   beats a stylesheet, so the CSS rule that dims the layer to 0.32 under
   prefers-reduced-motion could never win: the static frame sat at the full
   0.62. restOpacity() below leaves the inline value EMPTY under reduced
   motion, so the stylesheet's figure is the one that applies, and sets
   CFG.layer otherwise. That is the whole difference; the browser smoke
   (tests/browser/brand-stream.smoke.js) pins 0.32 so it does not come back.
   Everything else is as supplied, so a later drop-in replacement is a
   clean diff.
   ========================================================================== */
/*!
 * brand-stream.js — flowing data-stream background for Cygenix
 * Dependency-free, no build step, ~4KB minified.
 *
 * Creates a fixed, pointer-events:none <canvas> that sits alongside the existing
 * .brand-glow / .brand-grid decorative layers at z-index 0, and draws packets of
 * light travelling left-to-right along horizontal lanes.
 *
 * Behaviour: honours prefers-reduced-motion (renders one static frame, no rAF),
 * pauses on hidden tabs, and fades out + stops once the reader scrolls past the
 * hero, so nothing animates behind body copy.
 *
 * Exposes window.BrandStream = { CFG, apply(partialConfig) } for tuning.
 */
window.BrandStream = (function () {
  'use strict';

  // subtle:   laneGap 68, maxLanes 15, speedMin 30, speedMax 78,  trailMin 90,  trailMax 280, twoPacket 0.26, layer 0.42
  // balanced: laneGap 52, maxLanes 22, speedMin 40, speedMax 110, trailMin 100, trailMax 340, twoPacket 0.45, layer 0.62  <- current
  // bold:     laneGap 42, maxLanes 28, speedMin 52, speedMax 140, trailMin 120, trailMax 400, twoPacket 0.62, layer 0.80
  var CFG = {
    laneGap:   52,     // target px between lanes
    maxLanes:  22,
    speedMin:  40,     // px per second
    speedMax:  110,
    trailMin:  100,    // trail length, px
    trailMax:  340,
    twoPacket: 0.45,   // chance a lane carries a second packet
    layer:     0.62,   // master opacity of the whole layer
    rail:      0,      // alpha of static lane rails (0 = off)
    fadeAfter: 1.15    // fade out past this many viewport heights of scroll
  };

  // Sampled from the site's own tokens: --accent-ink, --accent, --teal.
  var COLORS = [
    '142,160,255', '142,160,255', '142,160,255',
    '74,91,214',   '74,91,214',
    '63,181,176'
  ];


  // Pre-rendered beam and head sprites — one per colour, built once. Drawing a
  // packet is then a single drawImage instead of two fresh gradients per frame,
  // which is what keeps this at ~60fps on integrated graphics.
  var SPRITE_W = 512, SPRITE_H = 12, HEAD = 24;
  var beams = [], heads = [];

  function makeSprites() {
    beams = []; heads = [];
    for (var i = 0; i < COLORS.length; i++) {
      var c = COLORS[i];

      var bc = document.createElement('canvas');
      bc.width = SPRITE_W; bc.height = SPRITE_H;
      var b = bc.getContext('2d');
      var g = b.createLinearGradient(0, 0, SPRITE_W, 0);
      g.addColorStop(0,    'rgba(' + c + ',0)');
      g.addColorStop(0.55, 'rgba(' + c + ',0.10)');
      g.addColorStop(0.88, 'rgba(' + c + ',0.42)');
      g.addColorStop(1,    'rgba(' + c + ',1)');
      b.fillStyle = g;
      b.globalAlpha = 0.30;
      b.fillRect(0, SPRITE_H / 2 - 2.6, SPRITE_W, 5.2);   // halo
      b.globalAlpha = 1;
      b.fillRect(0, SPRITE_H / 2 - 0.8, SPRITE_W, 1.6);   // core
      beams.push(bc);

      var hc = document.createElement('canvas');
      hc.width = HEAD; hc.height = HEAD;
      var hx = hc.getContext('2d');
      var hg = hx.createRadialGradient(HEAD / 2, HEAD / 2, 0, HEAD / 2, HEAD / 2, HEAD / 2);
      hg.addColorStop(0, 'rgba(' + c + ',0.55)');
      hg.addColorStop(1, 'rgba(' + c + ',0)');
      hx.fillStyle = hg;
      hx.fillRect(0, 0, HEAD, HEAD);
      hx.fillStyle = 'rgba(' + c + ',1)';
      hx.fillRect(HEAD / 2 - 3.5, HEAD / 2 - 1.2, 3.5, 2.4);  // bright head
      heads.push(hc);
    }
  }

  var reduced = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  var cv = document.createElement('canvas');
  cv.className = 'brand-stream';
  cv.setAttribute('aria-hidden', 'true');

  var anchor = document.querySelector('.brand-glow');
  if (anchor && anchor.parentNode) anchor.parentNode.insertBefore(cv, anchor.nextSibling);
  else document.body.insertBefore(cv, document.body.firstChild);

  var ctx = cv.getContext('2d', { alpha: true });
  var w = 0, h = 0, dpr = 1, lanes = [], raf = null, last = 0, visible = true;

  function rand(a, b) { return a + Math.random() * (b - a); }

  function packet(lane, seeded) {
    var len = rand(CFG.trailMin, CFG.trailMax);
    return {
      x:   seeded ? Math.random() * (w + len) : -len - Math.random() * w * 0.5,
      len: len,
      ci:  (Math.random() * COLORS.length) | 0,
      a:   0.34 + Math.random() * 0.66,
      sp:  lane.speed * (0.75 + Math.random() * 0.6)
    };
  }

  function build() {
    var count = Math.min(CFG.maxLanes, Math.max(6, Math.round(h / CFG.laneGap)));
    var stepY = h / count;
    lanes = [];
    for (var i = 0; i < count; i++) {
      var lane = {
        y: Math.round((i + 0.5) * stepY + rand(-stepY * 0.24, stepY * 0.24)) + 0.5,
        speed: rand(CFG.speedMin, CFG.speedMax),
        packets: []
      };
      lane.packets.push(packet(lane, true));
      if (Math.random() < CFG.twoPacket) lane.packets.push(packet(lane, true));
      lanes.push(lane);
    }
  }

  function resize() {
    dpr = Math.min(window.devicePixelRatio || 1, 1.5);
    w = window.innerWidth;
    h = window.innerHeight;
    cv.width  = Math.round(w * dpr);
    cv.height = Math.round(h * dpr);
    cv.style.width  = w + 'px';
    cv.style.height = h + 'px';
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    build();
    if (reduced) draw();
  }

  function draw() {
    ctx.clearRect(0, 0, w, h);
    ctx.globalCompositeOperation = 'lighter';

    if (CFG.rail > 0) {
      ctx.fillStyle = 'rgba(255,255,255,' + CFG.rail + ')';
      for (var r = 0; r < lanes.length; r++) ctx.fillRect(0, lanes[r].y, w, 1);
    }

    for (var i = 0; i < lanes.length; i++) {
      var lane = lanes[i];
      for (var j = 0; j < lane.packets.length; j++) {
        var p = lane.packets[j], x = p.x, y = lane.y;
        if (x < -HEAD || x - p.len > w) continue;
        ctx.globalAlpha = p.a;
        ctx.drawImage(beams[p.ci], x - p.len, y - SPRITE_H / 2, p.len, SPRITE_H);
        ctx.drawImage(heads[p.ci], x - HEAD / 2, y - HEAD / 2, HEAD, HEAD);
      }
    }

    ctx.globalAlpha = 1;
    ctx.globalCompositeOperation = 'source-over';
  }

  function step(t) {
    raf = requestAnimationFrame(step);
    var dt = last ? Math.min(0.05, (t - last) / 1000) : 0.016;
    last = t;
    for (var i = 0; i < lanes.length; i++) {
      var lane = lanes[i];
      for (var j = 0; j < lane.packets.length; j++) {
        var p = lane.packets[j];
        p.x += p.sp * dt;
        if (p.x - p.len > w) lane.packets[j] = packet(lane, false);
      }
    }
    draw();
  }

  function play() { if (reduced || raf !== null || !visible) return; last = 0; raf = requestAnimationFrame(step); }
  function pause() { if (raf !== null) { cancelAnimationFrame(raf); raf = null; } }

  // The opacity the layer rests at. Under prefers-reduced-motion the
  // stylesheet owns it (see the header), so no inline value is set at all.
  function restOpacity() { return reduced ? '' : String(CFG.layer); }

  var offTimer = null, faded = false;
  function onScroll() {
    var past = window.scrollY > window.innerHeight * CFG.fadeAfter;
    if (past === faded) return;
    faded = past;
    cv.style.opacity = past ? '0' : restOpacity();
    clearTimeout(offTimer);
    if (past) offTimer = setTimeout(pause, 700); else play();
  }

  document.addEventListener('visibilitychange', function () {
    visible = !document.hidden;
    if (visible && !faded) play(); else pause();
  });

  var rt = null;
  window.addEventListener('resize', function () { clearTimeout(rt); rt = setTimeout(resize, 150); }, { passive: true });
  window.addEventListener('scroll', onScroll, { passive: true });

  makeSprites();
  cv.style.opacity = restOpacity();
  resize();
  play();

  return {
    CFG: CFG,
    apply: function (o) { for (var k in o) CFG[k] = o[k]; cv.style.opacity = restOpacity(); resize(); }
  };
})();
