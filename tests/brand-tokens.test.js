// tests/brand-tokens.test.js — one home for the brand, one path to the
// accessibility engine.
//
// WHAT WENT WRONG
// A review arrived saying the homepage had drifted to a brick-red theme with
// pure-black headings and 45%-black hairlines, and asking for it to be
// rethemed against login.html. Every value quoted turned out to be the
// HIGH-CONTRAST accessibility theme — #9e2a1b appears exactly once in the
// repo, inside html.a11y-hc{} — read off a browser with the toggle on.
//
// The homepage had not drifted. But the report was still evidence of a real
// defect, twice over:
//
//   1. Seventeen pages had no path to cygenix-a11y.js at all. Turn high
//      contrast on at the homepage, click Sign in, and it silently vanished —
//      which is exactly the "two different companies" the reviewer saw, and is
//      an accessibility regression rather than a branding one.
//
//   2. The brand indigo really was written out five times, and the copies had
//      already begun to disagree: index spelled it #4a5bd6, help.html had lost
//      --accent2 and was setting a Helvetica stack while everything else used
//      IBM Plex Sans.
//
// Both are structural, so both are pinned here rather than left to be noticed
// again by someone reading colours off a screenshot.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const PUB = path.join(__dirname, '..', 'public');
const read = (f) => fs.readFileSync(path.join(PUB, f), 'utf8');
const pages = fs.readdirSync(PUB).filter((f) => f.endsWith('.html'));

console.log('Brand tokens and accessibility reach — one home, one path\n');

/* ── 1. The brand has one home ───────────────────────────────────────────── */

const brand = read('cygenix-brand.css');
const PUBLIC_PAGES = ['index.html', 'login.html', 'pricing.html', 'register.html', 'help.html'];

check('the shared file exists and declares the brand accent once',
  (brand.match(/--accent:/g) || []).length === 1 && /--accent:\s*#4A5BD6/i.test(brand));
check('with the hover accent, the glow, the type stacks and the radius',
  ['--accent2', '--accent-glow', '--sans', '--serif', '--mono', '--r']
    .every((t) => brand.indexOf(t + ':') !== -1));
check('and no surface colours — those legitimately differ per page',
  !/--bg\s*:/.test(brand) && !/--text\s*:/.test(brand),
  'index is black because a landing page reads better as one dark sheet; '
  + 'login is light because the product is. Forcing one set on both is a redesign.');

/* The block a page declares for itself. A scoped override further down the
   file — the landing page's product mockups carry the console's own palette —
   is deliberate and is not what this is looking for. */
const rootBlock = (src) => {
  const at = src.indexOf(':root{');
  return at === -1 ? '' : src.slice(at, src.indexOf('}', at));
};

PUBLIC_PAGES.forEach((f) => {
  const src = read(f);
  check(f + ' loads the shared tokens',
    /<link rel="stylesheet" href="\/cygenix-brand\.css">/.test(src));
  const root = rootBlock(src);
  const dupes = ['--accent', '--accent2', '--accent-glow', '--sans', '--serif', '--mono', '--r']
    .filter((t) => new RegExp(t + ':').test(root));
  check(f + ' does not keep its own copy of any of them',
    dupes.length === 0, dupes.join(', '));
});

check('every public page therefore renders IBM Plex Sans',
  PUBLIC_PAGES.every((f) => !/--serif:\s*'Helvetica/.test(read(f))),
  'help.html was on a Helvetica stack while every other page was on Plex');

/* The value the review mistook for the theme. It has exactly one legitimate
   home: the high-contrast mode, where a dark red is what meets contrast on
   white. Anywhere else it would be the drift the review thought it saw. */
const a11y = read('cygenix-a11y.js');
const brickRedFiles = pages.concat(['cygenix-a11y.js', 'cygenix-brand.css', 'cygenix-theme.css'])
  .filter((f) => read(f).indexOf('#9e2a1b') !== -1);
check('the high-contrast red lives only in the accessibility engine',
  brickRedFiles.length === 1 && brickRedFiles[0] === 'cygenix-a11y.js',
  brickRedFiles.join(', '));
check('and it is inside the html.a11y-hc block, not a page theme',
  /html\.a11y-hc\{[\s\S]{0,400}#9e2a1b/.test(a11y));

/* ── 2. The accessibility setting follows the visitor ────────────────────── */

// Three legitimate ways a page gets the engine: its own tag, the console
// sidebar (which lazy-loads it on mount), or nav.js on the older pages.
const hasA11y = (f) => {
  const src = read(f);
  return /cygenix-a11y/.test(src)
    || /cyg-sidebar-mount|cygenix-sidebar\.js/.test(src)
    || /src="\/nav\.js/.test(src);
};
const missing = pages.filter((f) => !hasA11y(f));
check('every page can load the accessibility engine',
  missing.length === 0,
  missing.join(', ') + ' — a setting turned on elsewhere would vanish here');

check('the sidebar still lazy-loads it, so console pages need no tag',
  /function ensureA11y\(\)/.test(read('cygenix-sidebar.js')));
check('a visitor who turns high contrast on at the homepage keeps it at sign-in',
  hasA11y('index.html') && hasA11y('login.html'),
  'this is the journey the review actually caught');
check('the engine re-applies the saved choice on load rather than only on click',
  /apply\(\);/.test(a11y) && /localStorage\.getItem\(KEY\)/.test(a11y));

// The panel needs something to open it. The console has the sidebar item;
// the public side has these.
check('and there is a way to reach the panel from the public pages',
  /a11y-trigger/.test(read('index.html')) && /a11y-trigger/.test(read('pricing.html')));

/* ── 3. The homepage stays black ─────────────────────────────────────────── */
//
// Recorded as a decision, not an accident: the retheme this work came from
// proposed moving index.html onto the login page's light neutral. It was
// declined deliberately — a dark marketing page in front of a light product is
// an ordinary pattern, and the accent already matched.

const index = read('index.html');
check('the landing page ground is still black',
  /--bg:\s*#000000/.test(rootBlock(index)), rootBlock(index).slice(0, 120));
check('and it still resolves the same accent as the sign-in page',
  !/--accent:/.test(rootBlock(index)) && !/--accent:/.test(rootBlock(read('login.html'))));

/* ── 4. The hygiene items ────────────────────────────────────────────────── */

check('the landing page declares a canonical url',
  /<link rel="canonical" href="https:\/\/cygenix\.co\.uk\/">/.test(index));
check('no page points og:image at a file that is not there',
  // The TODO comment names the tag it is telling you to restore, so match a
  // real tag — one carrying a content attribute — not the mention of one.
  !pages.some((f) => /<meta property="og:image"[^>]*content=/.test(read(f))),
  'a link preview with a missing image renders as a grey box — worse than no card');
check('and the twitter card was downgraded to match, not left claiming an image',
  !pages.some((f) => /twitter:card" content="summary_large_image"/.test(read(f))));
check('the heading outline goes h1 → h2 → h3 with nothing skipped',
  (() => {
    const order = (index.match(/<h([1-6])[ >]/g) || []).map((h) => Number(h[2]));
    return order[0] === 1 && order.every((lvl, i) => i === 0 || lvl - Math.min(...order.slice(0, i)) <= 2)
      && order.includes(2) && order.indexOf(2) < order.indexOf(3);
  })(),
  (index.match(/<h([1-6])[ >]/g) || []).join(' '));
check('the footer links Terms and Privacy, as every other page does',
  /<a href="\/terms">Terms<\/a>/.test(index) && /<a href="\/privacy">Privacy<\/a>/.test(index));
check('Contact goes somewhere real, so it still works with scripting off',
  /<a href="mailto:[^"]+"\s*\n?\s*onclick="openContact\(\)/.test(index)
  && !/<a href="#" onclick="openContact/.test(index));
check('the registered particulars are present, with the unknown values marked TODO',
  /Registered in England and Wales, company number/.test(index)
  && /TODO: company number/.test(index) && /TODO: registered office/.test(index),
  'inventing a company number would be a false statement about a legal entity');

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
