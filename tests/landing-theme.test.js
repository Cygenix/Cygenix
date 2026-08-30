// Tests for the landing page's theme.
//
// The page used to alternate light Redwood bands with dark brand bands, and an
// earlier version of this file held it to that rhythm. The page is now one
// surface — black, all the way down — so the rhythm rules are gone and the
// rules below take their place. What has NOT changed is the reason the file
// exists: index.html is the first thing anyone sees and the only page most
// visitors compare against the product, and it drifts unless something holds
// it to login.html, which is the brand's reference expression.
//
// Three things are asserted, in order of how badly they break the page:
//   1. It is genuinely black, everywhere, with no light band left behind.
//   2. It still shares the login's accent, ink, mark and buttons.
//   3. The product mockups are readable — they map back to the console's own
//      palette rather than trying to be dark.
const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};
const pub = (f) => fs.readFileSync(path.join(__dirname, '..', 'public', f), 'utf8');
const index = pub('index.html');
const login = pub('login.html');

console.log('Landing page — one black surface, same brand as the login\n');

const tok = (src, name) => {
  const m = new RegExp('--' + name + ':\\s*([^;}]+)').exec(src);
  return m ? m[1].trim().toLowerCase() : null;
};

// ── 1. Black, and black all the way down ────────────────────────────────────
check('the page ground is black',
  tok(index, 'bg') === '#000000', 'index --bg=' + tok(index, 'bg'));
check('both html and body paint it, so no strip of browser default shows through',
  /html\{[^}]*background:var\(--bg\)/.test(index) && /body\{[^}]*background:var\(--bg\)/.test(index));
check('the theme-color meta matches, so the mobile browser chrome does not flash white',
  /<meta name="theme-color" content="#000000">/.test(index));

// The old design earned its light sections. This one does not have any: a
// section that paints its own light background would be the drift.
const lightBand = /<section[^>]*class="[^"]*"[^>]*style="[^"]*background:\s*(#f|#e|#d|white|rgba\(255,255,255,0\.[3-9])/i;
check('no section paints itself a light background', !lightBand.test(index));
check('the old alternating-band classes are gone, not merely unused',
  !/section--ink/.test(index), 'section--ink still appears');

// Sections are separated by a hairline and space, not by a change of ground.
check('sections are divided by a hairline rule rather than a change of colour',
  /\.section\{[^}]*border-top:1px solid var\(--border\)/.test(index));

// ── 2. Nothing off-palette survives ─────────────────────────────────────────
// Two palettes are legitimate now: the black ground the page sits on, and the
// console's Redwood set, which appears only inside a .mock so a screenshot
// looks like a screenshot. Anything outside both is drift.
const BLACK_GROUND = ['#000000', '#0a0b0f', '#101219', '#171a22', '#f2f4f8',
  '#5fd08a', '#e3a33d', '#ef6f62', '#3fb5b0', '#a98fd6'];
const BRAND = ['#4a5bd6', '#3d4ec4', '#12141c', '#6d5df2', '#4a7cf3',
  '#8ea0ff', '#a9b6ff', '#9aa0af'];
const REDWOOD = ['#f3f4f5', '#ffffff', '#eceef0', '#e1e4e7', '#1a1d21',
  '#565b63', '#363d49', '#3f7d4e', '#b26a00', '#c0392b', '#0f6e6c', '#6b4e8e'];
const HOUSE = new Set([...BLACK_GROUND, ...BRAND, ...REDWOOD, '#fff', '#000']);
// An SVG fragment reference — url(#someId) — is not a colour, and an id whose
// letters happen to be hex digits would otherwise fail this as a stray. Blank
// those out before scanning rather than widening the palette to cover them.
const scannable = index.replace(/url\(#[^)]*\)/g, 'url()');
const stray = [...new Set((scannable.match(/#[0-9A-Fa-f]{3,8}\b/g) || []).map(h => h.toLowerCase()))]
  .filter(h => !HOUSE.has(h));
check('every colour on the page is a house colour', stray.length === 0, stray.join(', '));

// ── 3. The brand is still the login's brand ─────────────────────────────────
check('the accent is the same on both pages, and it is the console\'s accent',
  tok(index, 'accent') === tok(login, 'accent') && tok(index, 'accent') === '#4a5bd6',
  'index=' + tok(index, 'accent') + ' login=' + tok(login, 'accent'));
check('the brand ink is kept, so the dark surfaces still match the login',
  tok(index, 'ink') === tok(login, 'ink') && tok(index, 'ink') === '#12141c');
check('the brand mark gradient is the same pair of blues',
  tok(index, 'mark-a') === '#6d5df2' && tok(index, 'mark-b') === '#4a7cf3'
  && login.includes('#6d5df2') && login.includes('#4a7cf3'));
check('both pages round corners by the same radius token',
  tok(index, 'r') === tok(login, 'r'));

// #4A5BD6 does not carry type on black — the palette anticipated that with a
// lifted accent, and the page has to actually use it.
check('a lifted accent is defined for the black ground and is what the copy uses',
  tok(index, 'accent-ink') === '#8ea0ff'
  && /color:var\(--accent-ink\)/.test(index),
  'the brand accent is too dark to read on black');

const MARK = 'M9 10.5 14 16 9 21.5';
check('the landing page wears the same three-chevron mark as the login',
  index.includes(MARK) && login.includes(MARK)
  && /\.mark\{[^}]*linear-gradient\(140deg,var\(--mark-a\),var\(--mark-b\)\)/.test(index));
check('the mark links home and is reachable by name',
  /<a class="nav-logo" href="\/" aria-label="Cygenix home[^"]*">/.test(index));
/* The product is in beta and the nav says so. It has to say so to a screen
   reader too: the aria-label REPLACES the link's text, so a badge that is only
   visible would be invisible to anyone not looking at it. */
check('the nav marks the product as beta, in the palette rather than beside it',
  /<span class="nav-beta">Beta<\/span>/.test(index)
  && /\.nav-beta\{[^}]*color:var\(--text2\)/.test(index)
  && !/\.nav-beta\{[^}]*#[0-9a-fA-F]{3,6}/.test(index));
check('and the beta state reaches a screen reader, not just an eye',
  /aria-label="Cygenix home — the product is in beta"/.test(index));

// ── 4. The type scale matches the rest of the site ──────────────────────────
const h1 = /\.hero h1\{([^}]*)\}/.exec(index);
check('the headline is set at the login\'s weight and tracking, not heavier',
  h1 && /font-weight:700/.test(h1[1]) && /letter-spacing:-0\.03em/.test(h1[1]),
  h1 ? h1[1].slice(0, 90) : 'no .hero h1 rule');
check('no display type on the page is heavier than 700',
  !/font-weight:800/.test(index),
  (index.match(/[^;{]*font-weight:800[^;}]*/g) || []).slice(0, 2).join(' | '));

// ── 5. Buttons are the login's buttons ──────────────────────────────────────
check('the primary call to action uses the accent glow the login card uses',
  /\.btn-primary\{[^}]*box-shadow:0 6px 16px -6px rgba\(74,91,214,\.6\)/.test(index)
  && /\.btn-primary\{[^}]*box-shadow:0 6px 16px -6px rgba\(74,91,214,\.6\)/.test(login));
check('hero buttons stand at the login\'s 46px on the shared radius',
  /\.btn-hero\{[^}]*height:46px/.test(index) && /\.btn-hero\{[^}]*border-radius:var\(--r\)/.test(index));

// ── 6. The brand atmosphere, drawn once ─────────────────────────────────────
// The old page repeated the login's glow and grid per dark band. There are no
// bands now, so it is drawn once, fixed, behind everything — but it is the
// same four values, and login.html must still draw it the same way.
check('the login\'s two glows and its masked grid are still on the page',
  /radial-gradient\(760px 420px at 20% -10%/.test(index)
  && /radial-gradient\(620px 360px at 110% 110%/.test(index)
  && /mask-image:radial-gradient\(circle at 30% 40%/.test(index)
  && /background-size:52px 52px/.test(index),
  'these four values are copied from login.html — they must stay identical');
for (const frag of ['radial-gradient(760px 420px at 20% -10%',
                    'radial-gradient(620px 360px at 110% 110%',
                    'background-size:52px 52px']) {
  check('login.html still draws it that way too: ' + frag.slice(0, 34) + '…',
    login.includes(frag));
}
check('it is drawn once behind the page rather than repeated per section',
  /\.brand-glow\{[^}]*position:fixed/.test(index)
  && /\.brand-grid\{[^}]*position:fixed/.test(index));
check('the atmosphere is inert — it must never eat a click',
  /\.brand-glow\{[^}]*pointer-events:none/.test(index)
  && /\.brand-grid\{[^}]*pointer-events:none/.test(index));

// ── 7. The bar belongs to the hero until the hero is gone ───────────────────
check('the nav starts transparent over the hero and solidifies past it',
  /nav\{[^}]*background:transparent/.test(index)
  && /nav\.solid\{background:rgba\(0,0,0/.test(index)
  && /nav\.classList\.toggle\('solid'/.test(index)
  && /hero\.offsetHeight/.test(index));

// ── 8. The product is the only light on the page ────────────────────────────
// A mockup that tried to be dark would be a drawing of a product that does not
// exist. Inside .mock the tokens go back to the console's own palette.
check('a product mockup maps the tokens back — it shows the console as it is',
  /\.mock\{[\s\S]{0,400}?--text:#1A1D21/.test(index)
  && /\.mock\{[\s\S]{0,400}?--bg2:#FFFFFF/.test(index)
  && /\.mock\{[\s\S]{0,400}?--accent:#4A5BD6/.test(index));
check('the mockup window dots are neutral, not a borrowed traffic light',
  /\.mock-dot\{[^}]*background:var\(--border2\)/.test(index)
  && !/#f04070|#f5a623|#10d48a/.test(index));
check('a mockup is lifted off the black rather than floating with no edge',
  /\.mock\{[\s\S]{0,600}?box-shadow:0 30px 70px/.test(index));

// ── 9. Motion is optional ───────────────────────────────────────────────────
check('everything that moves stops for prefers-reduced-motion',
  /@media \(prefers-reduced-motion:reduce\)\{\*\{animation:none!important/.test(index));
check('the scroll reveal degrades to visible rather than to blank',
  /'IntersectionObserver' in window/.test(index)
  && /items\.forEach\(function\(el\)\{ el\.classList\.add\('in'\); \}\);/.test(index));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
