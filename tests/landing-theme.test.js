// Tests for the landing page's theme.
//
// index.html is the first thing anyone sees and the only page most visitors
// compare against the product. It had drifted: a heavier type scale than the
// rest of the site, no brand mark, no dark brand surface, and three separate
// palettes (the house tokens, a neon trio in the mockup chrome, and Tailwind's
// defaults in the sample report). These tests hold it to login.html, which is
// the brand's reference expression.
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

console.log('Landing page — same theme as the login and the console\n');

// ── 1. The two pages mix the same colours ───────────────────────────────────
const tok = (src, name) => {
  const m = new RegExp('--' + name + ':\\s*([^;}]+)').exec(src);
  return m ? m[1].trim().toLowerCase() : null;
};
check('the accent is the same on both pages, and it is the console\'s accent',
  tok(index, 'accent') === tok(login, 'accent') && tok(index, 'accent') === '#4a5bd6',
  index_v(tok(index, 'accent'), tok(login, 'accent')));
function index_v(a, b) { return 'index=' + a + ' login=' + b; }
check('the landing page adopts the login\'s ink, so the dark surfaces match',
  tok(index, 'ink') === tok(login, 'ink') && tok(index, 'ink') === '#12141c');
check('the brand mark gradient is the same pair of blues',
  tok(index, 'mark-a') === '#6d5df2' && tok(index, 'mark-b') === '#4a7cf3'
  && login.includes('#6d5df2') && login.includes('#4a7cf3'));
check('both pages round corners by the same radius token',
  tok(index, 'r') === tok(login, 'r'));

// ── 2. Nothing off-palette survives ─────────────────────────────────────────
// The exceptions are deliberate and named: pure black/white, and the three
// syntax colours in the SQL code sample, which imitate an editor rather than
// the product.
const HOUSE = new Set(['#4a5bd6', '#3d4ec4', '#12141c', '#6d5df2', '#4a7cf3',
  '#8ea0ff', '#a9b6ff', '#9aa0af', '#f3f4f5', '#ffffff', '#eceef0', '#e1e4e7',
  '#1a1d21', '#565b63', '#363d49', '#3f7d4e', '#b26a00', '#c0392b', '#0f6e6c',
  '#6b4e8e', '#fff', '#000', '#fea']);
const SYNTAX = new Set(['#dcdcaa', '#ce9178', '#b5cea8', '#569cd6', '#6a9955']);  // the SQL sample imitates an editor
const stray = [...new Set((index.match(/#[0-9A-Fa-f]{3,8}\b/g) || []).map(h => h.toLowerCase()))]
  .filter(h => !HOUSE.has(h) && !SYNTAX.has(h));
check('every colour on the page is a house colour', stray.length === 0, stray.join(', '));

// ── 3. The type scale matches the rest of the site ──────────────────────────
// The drift was weight 800 at -0.04em — a full step heavier and tighter than
// anything else Cygenix sets, which is what made the page look borrowed.
const h1 = /\.hero h1\{([^}]*)\}/.exec(index);
check('the headline is set at the login\'s weight and tracking, not heavier',
  h1 && /font-weight:700/.test(h1[1]) && /letter-spacing:-0\.03em/.test(h1[1]),
  h1 ? h1[1].slice(0, 90) : 'no .hero h1 rule');
check('no display type on the page is heavier than 700',
  !/font-weight:800/.test(index),
  (index.match(/[^;{]*font-weight:800[^;}]*/g) || []).slice(0, 2).join(' | '));

// ── 4. The hero IS the brand surface, drawn the same way login draws it ─────
const hero = /\.hero\{([^}]*)\}/.exec(index);
check('the hero sits on the brand ink rather than the page background',
  hero && /background:var\(--ink\)/.test(hero[1]));
check('it carries the login panel\'s two radial glows and its masked grid',
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

// ── 5. The brand mark, not a bare wordmark ──────────────────────────────────
const MARK = 'M9 10.5 14 16 9 21.5';
check('the landing page wears the same three-chevron mark as the login',
  index.includes(MARK) && login.includes(MARK)
  && /\.mark\{[^}]*linear-gradient\(140deg,var\(--mark-a\),var\(--mark-b\)\)/.test(index));
check('the mark links home and is reachable by name',
  /<a class="nav-logo" href="\/" aria-label="Cygenix home">/.test(index));

// ── 6. Buttons are the login's buttons ──────────────────────────────────────
check('the primary call to action uses the accent glow the login card uses',
  /\.btn-primary\{[^}]*box-shadow:0 6px 16px -6px rgba\(74,91,214,\.6\)/.test(index)
  && /\.btn-primary\{[^}]*box-shadow:0 6px 16px -6px rgba\(74,91,214,\.6\)/.test(login));
check('hero buttons stand at the login\'s 46px on the shared radius',
  /\.btn-hero\{[^}]*height:46px/.test(index) && /\.btn-hero\{[^}]*border-radius:var\(--r\)/.test(index));

// ── 7. The bar belongs to the hero until the hero is gone ───────────────────
check('the nav starts transparent over the hero and solidifies past it',
  /nav\{[^}]*background:transparent/.test(index)
  && /nav\.solid\{background:var\(--bg2\)/.test(index)
  && /nav\.classList\.toggle\('solid'/.test(index)
  && /hero\.offsetHeight/.test(index),
  'a white band above the ink would cut the page in two');

// ── 8. Mockup chrome is Cygenix, not macOS ──────────────────────────────────
check('the mockup window dots are neutral, not a borrowed traffic light',
  /\.mock-dot\{[^}]*background:var\(--border2\)/.test(index)
  && !/#f04070|#f5a623|#10d48a/.test(index));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
