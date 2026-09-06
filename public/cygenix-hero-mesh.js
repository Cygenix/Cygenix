/* ============================================================================
   cygenix-hero-mesh.js — mounts the polygon mesh behind a page's hero.
   ----------------------------------------------------------------------------
   The engine (cygenix-mesh.js) draws; this decides where and with what. It
   started as an inline script in index.html. When the pricing page took the
   landing page's theme it needed the same mount with the same values, and
   two copies of a tuning block are two copies that drift — so the mount is
   one file, loaded by every page that has a hero with a #cx-mesh canvas.

   It runs after DOMContentLoaded (deferred scripts, the engine included,
   have all executed by then) and is a no-op wherever #cx-mesh is absent, so
   loading it on a page without a hero costs one lookup.

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

   window.cygenixHeroMesh keeps the handle, so the console can tune live:
   cygenixHeroMesh.update({ speed: 0.3, reach: 220 }).
   ========================================================================== */
document.addEventListener('DOMContentLoaded', function () {
  var el = document.getElementById('cx-mesh');
  if (!el || !window.CygenixMesh) return;
  var tok = function (name, fallback) {
    var v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return /^#[0-9a-f]{3,6}$/i.test(v) ? v : fallback;
  };
  window.cygenixHeroMesh = CygenixMesh.mount(el, {
    density:   1.45,   // denser field — more polygon closure
    speed:     1.0,    // tuned live: below ~0.5 it did not read as motion
    reach:     200,    // longer connections so triangles actually form
    glow:      0.80,   // blue bloom behind the headline
    parallax:  0.55,   // layer shift against the pointer and the camera sweep
    lineAlpha: 0.50,   // link opacity, tuned live with the brighter connector colour
    nodeAlpha: 0.95,
    node:      tok('--accent-ink2', '#a9b6ff'),
    line:      tok('--accent-ink', '#8ea0ff'),
    glowColor: tok('--accent', '#4a5bd6')
  });
});
