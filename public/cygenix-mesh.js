/* ============================================================================
   cygenix-mesh.js — the ambient polygon mesh behind the landing page's hero.
   ----------------------------------------------------------------------------
   WHAT IT IS
   Drifting nodes joined by fading lines, in three depth layers — the far one
   drawn at half resolution and blurred, the near ones sharp — over a faint
   blue bloom centred behind the headline, the whole field shifting gently
   against the pointer. It is decoration for one section of one page, and it
   is held to the standard decoration on the first page anyone sees deserves.

   WHY IT IS A FILE OF ITS OWN
   index.html carries its own styles and its own page logic inline, and the
   shared modules it loads are separate deferred files stamped with a content
   hash by scripts/stamp-assets.js. This is the second kind: a self-contained
   engine with an API (mount / update / destroy), large enough that a stale
   cached copy would matter. Keeping it out of the page also keeps
   tests/landing-theme.test.js — which scans index.html for colours outside
   the house palette — looking at the page's colours rather than at the
   engine's arithmetic. Its CSS is the exception and lives inline in
   index.html next to the .hero rules, where the page keeps its styles.

   WHERE IT SITS, AND THE ONE THING THE BRIEF GOT WRONG
   The brief said to put the layer inside section.hero. On the live page
   .hero is a 1000px box centred in the viewport, not a full-bleed band, so
   a mesh clipped to it would stop dead at two hard vertical edges on any
   screen wider than that. The hero is therefore wrapped in .hero-stage — a
   full-width, position:relative, overflow:hidden box that adds no padding,
   no margin and no height — and the layer is the stage's first child. The
   hero's own size and position are unchanged (tests/browser/hero-mesh.smoke.js
   measures it with the layer shown and hidden, and the two must match).

   It sits ABOVE the page's fixed .brand-glow and .brand-grid, which stay
   underneath as they were: the mesh is additive.

   WHAT IT MUST NEVER DO
   Change the hero's height or spacing, cross in front of the copy, eat a
   click, keep drawing when nobody can see it, or start a loop for a reader
   who asked for reduced motion. So: the layer is pointer-events:none and
   under the copy; an IntersectionObserver stops the loop when the hero is
   scrolled out of view and visibilitychange stops it on a hidden tab;
   prefers-reduced-motion draws one frame and never schedules a
   requestAnimationFrame; devicePixelRatio is capped at 2. The mount call in
   index.html is guarded so it is a no-op wherever #cx-mesh is absent.

   INTENSITY
   The mount call uses the "balanced" preset's values, written out so they
   can be tuned one at a time. PRESETS below holds subtle and cinematic too;
   pass { preset: 'subtle' } to update() or mount() to switch wholesale.

   The engine below is as it was supplied, verbatim; this header is the only
   addition. Keep it that way so a later drop-in replacement is a clean diff.
   ========================================================================== */
/* Cygenix ambient mesh — animated polygon network background.
   window.CygenixMesh.mount(canvas, opts) -> { update(opts), destroy() } */
(function () {
  var PRESETS = {
    subtle:    { density: 0.55, speed: 0.45, reach: 140, glow: 0.35, parallax: 0.30, lineAlpha: 0.20, nodeAlpha: 0.60 },
    balanced:  { density: 1.00, speed: 0.75, reach: 165, glow: 0.65, parallax: 0.55, lineAlpha: 0.30, nodeAlpha: 0.85 },
    cinematic: { density: 1.60, speed: 1.05, reach: 190, glow: 1.05, parallax: 0.90, lineAlpha: 0.42, nodeAlpha: 1.00 }
  };
  var DEPTHS = [0.34, 0.66, 1.0];
  var SHARE  = [0.42, 0.33, 0.25];

  function hexRGB(h) {
    h = h.replace('#', '');
    if (h.length === 3) h = h[0] + h[0] + h[1] + h[1] + h[2] + h[2];
    var n = parseInt(h, 16);
    return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
  }

  function mount(canvas, opts) {
    var cfg = Object.assign({
      preset: 'balanced',
      node: '#a9b6ff',
      line: '#4a5bd6',
      glowColor: '#4a5bd6',
      maxDPR: 2,
      paused: false
    }, PRESETS[(opts && opts.preset) || 'balanced'], opts || {});

    var host = canvas.parentElement || canvas;
    var ctx = canvas.getContext('2d', { alpha: true });
    var far = document.createElement('canvas');
    var fctx = far.getContext('2d', { alpha: true });

    var W = 0, H = 0, dpr = 1;
    var layers = [];
    var pointer = { x: 0, y: 0, tx: 0, ty: 0 };
    var raf = 0, last = 0, running = false, visible = true;
    var reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    function area() { return W * H; }

    function build() {
      var total = Math.round(Math.min(420, Math.max(24, (area() / 4600) * cfg.density)));
      layers = DEPTHS.map(function (d, i) {
        var n = Math.max(4, Math.round(total * SHARE[i]));
        var pts = [];
        for (var k = 0; k < n; k++) {
          var ang = Math.random() * Math.PI * 2;
          var v = (0.055 + d * 0.075) * cfg.speed;
          pts.push({
            x: Math.random() * W,
            y: Math.random() * H,
            vx: Math.cos(ang) * v,
            vy: Math.sin(ang) * v * 0.7,
            r: 0.7 + d * 1.5 * (0.6 + Math.random() * 0.8),
            tw: Math.random() * Math.PI * 2,
            ts: 0.006 + Math.random() * 0.012
          });
        }
        return { d: d, pts: pts, reach: cfg.reach * (0.55 + d * 0.55) };
      });
    }

    function resize() {
      var r = host.getBoundingClientRect();
      var w = Math.max(1, Math.round(r.width));
      var h = Math.max(1, Math.round(r.height));
      if (w === W && h === H) return;
      W = w; H = h;
      dpr = Math.min(cfg.maxDPR, window.devicePixelRatio || 1);
      canvas.width = Math.round(W * dpr);
      canvas.height = Math.round(H * dpr);
      canvas.style.width = W + 'px';
      canvas.style.height = H + 'px';
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      far.width = Math.max(1, Math.round(W * dpr * 0.5));
      far.height = Math.max(1, Math.round(H * dpr * 0.5));
      fctx.setTransform(dpr * 0.5, 0, 0, dpr * 0.5, 0, 0);
      build();
      if (!running) draw(16);
    }

    function step(pts, dt) {
      var m = 60;
      for (var i = 0; i < pts.length; i++) {
        var p = pts[i];
        p.x += p.vx * dt; p.y += p.vy * dt; p.tw += p.ts * dt;
        if (p.x < -m) p.x = W + m; else if (p.x > W + m) p.x = -m;
        if (p.y < -m) p.y = H + m; else if (p.y > H + m) p.y = -m;
      }
    }

    function renderLayer(c, layer, ox, oy, nodeRGB, lineRGB) {
      var pts = layer.pts, reach = layer.reach, r2 = reach * reach;
      var cell = reach, cols = Math.ceil((W + 2 * cell) / cell), rows = Math.ceil((H + 2 * cell) / cell);
      var grid = new Array(cols * rows);
      var i, p, gx, gy, idx;
      for (i = 0; i < pts.length; i++) {
        p = pts[i];
        gx = Math.min(cols - 1, Math.max(0, Math.floor((p.x + cell) / cell)));
        gy = Math.min(rows - 1, Math.max(0, Math.floor((p.y + cell) / cell)));
        idx = gy * cols + gx;
        (grid[idx] || (grid[idx] = [])).push(p);
      }

      c.lineWidth = 0.6 + layer.d * 0.5;
      var baseLine = cfg.lineAlpha * (0.4 + layer.d * 0.75);
      for (gy = 0; gy < rows; gy++) {
        for (gx = 0; gx < cols; gx++) {
          var a = grid[gy * cols + gx];
          if (!a) continue;
          for (var dy = 0; dy <= 1; dy++) {
            for (var dx = (dy === 0 ? 0 : -1); dx <= 1; dx++) {
              var nx = gx + dx, ny = gy + dy;
              if (nx < 0 || ny < 0 || nx >= cols || ny >= rows) continue;
              var b = grid[ny * cols + nx];
              if (!b) continue;
              for (i = 0; i < a.length; i++) {
                var s = (a === b) ? i + 1 : 0;
                for (var j = s; j < b.length; j++) {
                  var q = a[i], w = b[j];
                  if (q === w) continue;
                  var ddx = q.x - w.x, ddy = q.y - w.y, d2 = ddx * ddx + ddy * ddy;
                  if (d2 > r2) continue;
                  var t = 1 - Math.sqrt(d2) / reach;
                  c.strokeStyle = 'rgba(' + lineRGB + ',' + (baseLine * t * t).toFixed(3) + ')';
                  c.beginPath();
                  c.moveTo(q.x + ox, q.y + oy);
                  c.lineTo(w.x + ox, w.y + oy);
                  c.stroke();
                }
              }
            }
          }
        }
      }

      c.globalCompositeOperation = 'lighter';
      var baseNode = cfg.nodeAlpha * (0.35 + layer.d * 0.8);
      for (i = 0; i < pts.length; i++) {
        p = pts[i];
        var tw = 0.72 + 0.28 * Math.sin(p.tw);
        c.fillStyle = 'rgba(' + nodeRGB + ',' + (baseNode * tw).toFixed(3) + ')';
        c.beginPath();
        c.arc(p.x + ox, p.y + oy, p.r, 0, 6.2832);
        c.fill();
      }
      c.globalCompositeOperation = 'source-over';
    }

    var nodeRGB, lineRGB, glowRGB;
    function recolor() {
      nodeRGB = hexRGB(cfg.node).join(',');
      lineRGB = hexRGB(cfg.line).join(',');
      glowRGB = hexRGB(cfg.glowColor).join(',');
    }
    recolor();

    function draw(dt) {
      pointer.x += (pointer.tx - pointer.x) * 0.045;
      pointer.y += (pointer.ty - pointer.y) * 0.045;

      ctx.clearRect(0, 0, W, H);

      if (cfg.glow > 0) {
        var g = ctx.createRadialGradient(W * 0.5, H * 0.42, 0, W * 0.5, H * 0.42, Math.max(W, H) * 0.62);
        g.addColorStop(0, 'rgba(' + glowRGB + ',' + (0.17 * cfg.glow).toFixed(3) + ')');
        g.addColorStop(0.55, 'rgba(' + glowRGB + ',' + (0.045 * cfg.glow).toFixed(3) + ')');
        g.addColorStop(1, 'rgba(' + glowRGB + ',0)');
        ctx.fillStyle = g;
        ctx.fillRect(0, 0, W, H);
      }

      var l0 = layers[0];
      fctx.clearRect(0, 0, W, H);
      renderLayer(fctx, l0, pointer.x * cfg.parallax * 10, pointer.y * cfg.parallax * 8, nodeRGB, lineRGB);
      var prev = ctx.filter;
      ctx.filter = 'blur(1.6px)';
      ctx.drawImage(far, 0, 0, W, H);
      ctx.filter = prev || 'none';

      renderLayer(ctx, layers[1], pointer.x * cfg.parallax * 20, pointer.y * cfg.parallax * 15, nodeRGB, lineRGB);
      renderLayer(ctx, layers[2], pointer.x * cfg.parallax * 34, pointer.y * cfg.parallax * 26, nodeRGB, lineRGB);
    }

    function frame(now) {
      raf = requestAnimationFrame(frame);
      var dt = Math.min(3, (now - last) / 16.6667 || 1);
      last = now;
      for (var i = 0; i < layers.length; i++) step(layers[i].pts, dt);
      draw(dt);
    }

    function start() {
      if (running || reduced || cfg.paused || !visible) return;
      running = true; last = performance.now();
      raf = requestAnimationFrame(frame);
    }
    function stop() { running = false; cancelAnimationFrame(raf); }

    function onPointer(e) {
      if (!cfg.parallax) return;
      var r = host.getBoundingClientRect();
      pointer.tx = ((e.clientX - r.left) / r.width - 0.5) * 2;
      pointer.ty = ((e.clientY - r.top) / r.height - 0.5) * 2;
    }
    function onLeave() { pointer.tx = 0; pointer.ty = 0; }
    function onVis() { visible = !document.hidden && inView; visible ? start() : stop(); }

    var inView = true;
    var io = null;
    if ('IntersectionObserver' in window) {
      io = new IntersectionObserver(function (es) {
        inView = es[0].isIntersecting;
        onVis();
      }, { threshold: 0 });
      io.observe(host);
    }
    var ro = null;
    if ('ResizeObserver' in window) { ro = new ResizeObserver(resize); ro.observe(host); }
    else window.addEventListener('resize', resize);

    document.addEventListener('visibilitychange', onVis);
    window.addEventListener('pointermove', onPointer, { passive: true });
    host.addEventListener('pointerleave', onLeave);

    resize();
    if (reduced) draw(1); else start();

    return {
      update: function (next) {
        Object.assign(cfg, next && next.preset ? PRESETS[next.preset] : {}, next || {});
        recolor(); build();
        if (cfg.paused) stop(); else start();
        if (!running) draw(1);
      },
      config: cfg,
      destroy: function () {
        stop();
        if (io) io.disconnect();
        if (ro) ro.disconnect(); else window.removeEventListener('resize', resize);
        document.removeEventListener('visibilitychange', onVis);
        window.removeEventListener('pointermove', onPointer);
        host.removeEventListener('pointerleave', onLeave);
      }
    };
  }

  window.CygenixMesh = { mount: mount, presets: PRESETS };
})();
