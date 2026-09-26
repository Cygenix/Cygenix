// Asset stamping — deployed fixes must actually reach the browser.
//
// public/*.js is served with a stale-while-revalidate window, and the HTML
// that references it is always revalidated. With an UNVERSIONED script URL
// that combination serves a returning browser its cached (stale) copy and
// only fetches the new one in the background — so a shipped fix could sit
// unseen behind a cached script. That is not hypothetical: it is how a fixed
// jobs-list crash kept happening for a user after the fix was live.
//
// scripts/stamp-assets.js gives every local <script src> a ?v=<content hash>,
// so a changed file always has a changed URL and the browser must fetch it.
const fs = require('fs');
const path = require('path');
const { stampHtml, hashOf, run } = require('../scripts/stamp-assets.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Asset stamping — cache-busting for deployed fixes\n');

// ── The rewrite itself ──────────────────────────────────────────────────────
{
  const H = (f) => ({ 'a.js': 'aaa111', 'b.js': 'bbb222' })[f] || null;

  let r = stampHtml('<script src="/a.js" defer></script>', H);
  check('an unstamped local script gains its content hash',
    r.out === '<script src="/a.js?v=aaa111" defer></script>', r.out);
  check('and that counts as a change', r.changed === true);

  r = stampHtml('<script src="/a.js?v=aaa111" defer></script>', H);
  check('a current stamp is left exactly as it is', r.changed === false, r.out);

  r = stampHtml('<script src="/a.js?v=OLDHASH" defer></script>', H);
  check('a stale stamp is replaced, not appended',
    r.out === '<script src="/a.js?v=aaa111" defer></script>', r.out);

  r = stampHtml('<script src="https://cdn.example.com/x.js"></script>', H);
  check('a third-party script is never touched', r.changed === false, r.out);

  r = stampHtml('<script src="/missing.js"></script>', H);
  check('a src with no file on disk is left alone rather than guessed',
    r.changed === false && r.missing.includes('missing.js'));

  r = stampHtml('<script src="/a.js"></script>\n<script src="/b.js"></script>', H);
  check('every script on a page is stamped',
    r.out.includes('a.js?v=aaa111') && r.out.includes('b.js?v=bbb222'));

  // Attribute order varies across these pages.
  r = stampHtml('<script defer src="/a.js"></script>', H);
  check('attribute order does not matter', r.out.includes('/a.js?v=aaa111'), r.out);

  // An inline script that names a local script by a quoted root-relative
  // path is a runtime injection, and with a year-long cache it must be
  // stamped too — that is the case the short header window used to cover.
  const inline = '<script>\nvar s = document.createElement("script"); s.src = "/a.js";\n</script>';
  check('a runtime injection inside an inline script is stamped', /s\.src = "\/a\.js\?v=aaa111"/.test(stampHtml(inline, H).out), stampHtml(inline, H).out);
  check('but prose that merely mentions a path in a longer string is not',
    stampHtml('<script>var t = "see /a.js for details";</script>', H).changed === false);
  check('a stylesheet link is stamped like a script',
    stampHtml('<link rel="stylesheet" href="/a.css">', (f) => (f === 'a.css' ? 'ccc333' : H(f))).out.includes('/a.css?v=ccc333'));
  const { stampJs } = require('../scripts/stamp-assets.js');
  check('a JS module that injects another names it by its stamped URL',
    stampJs("s.src = '/a.js'; inject('id', '/b.js?v=OLD');", H).out === "s.src = '/a.js?v=aaa111'; inject('id', '/b.js?v=bbb222');");
}

// ── Hash behaviour ──────────────────────────────────────────────────────────
{
  const tmp = path.join(__dirname, '..', 'public', 'cygenix-sidebar.js');
  check('a hash is short enough for a URL and stable',
    /^[a-f0-9]{10}$/.test(hashOf(tmp)) && hashOf(tmp) === hashOf(tmp));
}

// ── The repo is currently in sync ───────────────────────────────────────────
{
  check('every committed page carries current stamps (run: node scripts/stamp-assets.js)',
    run({ check: true }) === 0);
}

// ── Wiring ──────────────────────────────────────────────────────────────────
{
  const toml = fs.readFileSync(path.join(__dirname, '..', 'netlify.toml'), 'utf8');
  // The build command grew a second generator (build-routes). Assert that
  // stamping is IN it rather than that it is the whole of it, so adding a
  // third step does not fail a test about the first.
  check('the build stamps, so a deploy cannot ship mismatched URLs',
    /command = "[^"]*node scripts\/stamp-assets\.js/.test(toml),
    (toml.match(/command = "[^"]*"/) || [''])[0]);
  // Scripts and stylesheets are cached for a year and marked immutable. That
  // is safe for exactly as long as every long-lived URL is stamped, so the
  // three scans that follow are the other half of this header.
  const cc = (glob) => (toml.match(new RegExp('for = "' + glob.replace(/[*.]/g, '\\$&') + '"[\\s\\S]*?Cache-Control = "([^"]*)"')) || [])[1] || '';
  check('scripts are cached for a year, immutable', cc('/*.js') === 'public, max-age=31536000, immutable', cc('/*.js'));
  check('so are stylesheets', cc('/*.css') === 'public, max-age=31536000, immutable', cc('/*.css'));
  check('and HTML is still revalidated on every navigation, which is what carries the new stamps',
    cc('/*.html') === 'public, max-age=0, must-revalidate', cc('/*.html'));
  check('the change altered values, not the number of header blocks', (toml.match(/\[\[headers\]\]/g) || []).length === 5);

  const PUB = path.join(__dirname, '..', 'public');
  const pages = fs.readdirSync(PUB).filter((f) => f.endsWith('.html'));
  const mods = fs.readdirSync(PUB).filter((f) => f.endsWith('.js'));
  const bare = [];
  for (const f of pages) {
    const h = fs.readFileSync(path.join(PUB, f), 'utf8');
    for (const m of h.matchAll(/<link\b[^>]*\bhref="\/([A-Za-z0-9._-]+\.css)"/g)) bare.push(f + ' <link ' + m[1]);
    for (const m of h.matchAll(/<script\b[^>]*\bsrc="\/([A-Za-z0-9._-]+\.js)"/g)) bare.push(f + ' <script ' + m[1]);
    for (const m of h.matchAll(/(['"])\/([A-Za-z0-9._-]+\.js)\1/g)) bare.push(f + ' literal ' + m[2]);
  }
  for (const f of mods) {
    const j = fs.readFileSync(path.join(PUB, f), 'utf8');
    // A file naming ITSELF (five do, in a header comment showing how to
    // include them) is exempt, as it is in the stamper: it can never carry
    // a stamp for its own content, and it is never an injection.
    for (const m of j.matchAll(/(['"])\/([A-Za-z0-9._-]+\.js)\1/g)) if (m[2] !== f) bare.push(f + ' literal ' + m[2]);
  }
  check('NO LONG-LIVED URL IS REACHED WITHOUT A STAMP — no bare <script src>, <link href> or injected path anywhere in public/',
    bare.length === 0, bare.slice(0, 8).join(', '));
  check('the runtime injections in particular carry stamps',
    /s\.src = '\/server-migration\.js\?v=[a-f0-9]{10}'/.test(fs.readFileSync(path.join(PUB, 'dashboard-app.js'), 'utf8'))
    && /'\/cygenix-drive-modal\.js\?v=[a-f0-9]{10}'/.test(fs.readFileSync(path.join(PUB, 'cygenix-sidebar.js'), 'utf8')));

  const pb = fs.readFileSync(path.join(__dirname, '..', 'public', 'project-builder.html'), 'utf8');
  check('the page whose fix was cached now points at a versioned URL',
    /project-builder-app\.js\?v=[a-f0-9]{10}/.test(pb));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
