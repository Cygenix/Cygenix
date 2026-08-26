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

  // Inline scripts have no src and must survive untouched.
  const inline = '<script>\nvar x = "/a.js";\n</script>';
  check('inline script bodies are not rewritten', stampHtml(inline, H).out === inline);
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
  // Runtime-injected scripts have no tag to stamp and rely on the header
  // alone, so the stale window must stay short.
  check('the stale-while-revalidate window is bounded for unstampable scripts',
    !/stale-while-revalidate=86400/.test(toml) && /stale-while-revalidate=600/.test(toml));

  const pb = fs.readFileSync(path.join(__dirname, '..', 'public', 'project-builder.html'), 'utf8');
  check('the page whose fix was cached now points at a versioned URL',
    /project-builder-app\.js\?v=[a-f0-9]{10}/.test(pb));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
