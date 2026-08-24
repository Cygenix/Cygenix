// Tests for the shared icon language.
//
// The console has one icon style: the 16×16 stroked line icons the nav rail
// draws inline, shipped to every other surface as CSS masks in
// cygenix-icons.css. The stakes are consistency and honesty — an emoji is a
// different typeface on every operating system, changes colour independently
// of the button it sits in, and cannot be themed; a mask icon inherits
// currentColor like the nav does. These tests keep emoji from drifting back,
// and keep the generated stylesheet in step with its source table.
const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};
const P = (...p) => path.join(__dirname, '..', ...p);
const pub = (f) => fs.readFileSync(P('public', f), 'utf8');
const pages = fs.readdirSync(P('public')).filter(f => f.endsWith('.html'));
const scripts = fs.readdirSync(P('public')).filter(f => f.endsWith('.js'));

console.log('Icons — one language across the console\n');

// ── 1. The generated stylesheet is in step with its source table ────────────
const before = pub('cygenix-icons.css');
execFileSync('node', [P('scripts', 'build-icons.js')], { stdio: 'pipe' });
check('the committed stylesheet is exactly what the generator produces',
  pub('cygenix-icons.css') === before,
  're-run node scripts/build-icons.js and commit the result');

const css = pub('cygenix-icons.css');
check('the base class paints the mask with the surrounding text colour',
  /\.ic\{[^}]*background-color:currentColor/.test(css)
  && /\.ic\{[^}]*mask-size:contain/.test(css)
  && /-webkit-mask-image/.test(css),
  'without currentColor an icon cannot follow a red button or a muted toolbar');
check('every icon is drawn on the nav rail\'s grid, in its stroke weight',
  (css.match(/viewBox%3D%220%200%2016%2016%22/g) || []).length >= 60
  && /stroke-width%3D%221\.[23]%22/.test(css));

// ── 2. No emoji left in the console's chrome ────────────────────────────────
// Flags stay: a country is not a line icon. Monochrome typographic marks
// (→ ✓ ✕ ▾ ●) stay too — they are type, they already inherit colour.
const EMOJI = /[\u{1F300}-\u{1FAFF}\u{2B00}-\u{2BFF}\u{2600}-\u{27BF}\u{2300}-\u{23FF}]/u;
const KEEP = new Set([...'→←↑↓↔↗↘↖↙↻↺⇅⇄⇆↩⇦⇨✓✕✗✎✏▾▴▸◂▶▼▲●○◆◇■□⌄⌃×·−✱❮❯◈★☆⌘⛶']);
const FLAG = /[\u{1F1E6}-\u{1F1FF}]/u;
const offenders = [];
for (const f of [...pages, ...scripts]) {
  if (f === 'cygenix-region.js') continue;         // flags, by definition
  const lines = pub(f).split('\n');
  lines.forEach((ln, i) => {
    const t = ln.trim();
    if (/^(\*|\/\/|\/\*|<!--)/.test(t)) return;    // comments are not chrome
    for (const ch of ln) {
      if (KEEP.has(ch) || FLAG.test(ch)) continue;
      if (EMOJI.test(ch)) offenders.push(f + ':' + (i + 1) + ' ' + ch + ' ' + t.slice(0, 70));
    }
  });
}
check('no emoji survive in page markup or the scripts that render it',
  offenders.length === 0, offenders.slice(0, 6).join(' | '));

// ── 3. Every page can actually draw the icons ───────────────────────────────
// The sidebar, the cookie banner and the integrations drawer render on every
// page, so the sheet belongs on every page — not only the ones whose own
// markup happens to use it.
const noSheet = pages.filter(f => !pub(f).includes('cygenix-icons.css'));
check('every page links the icon stylesheet', noSheet.length === 0, noSheet.join(', '));

// ── 4. Icons are only ever used where markup renders ────────────────────────
const misplaced = [];
for (const f of [...pages, ...scripts]) {
  pub(f).split('\n').forEach((ln, i) => {
    const attr = /\b(title|aria-label|placeholder|alt)\s*=\s*("[^"]*"|'[^']*')/g;
    let m;
    while ((m = attr.exec(ln))) {
      if (/<i class=.?ic/.test(m[2])) misplaced.push(f + ':' + (i + 1) + ' ' + m[1]);
    }
    if (/\b(alert|confirm|prompt)\s*\([^)]*<i class/.test(ln)) misplaced.push(f + ':' + (i + 1) + ' dialog');
    if (/\.(textContent|innerText)\s*=[^;]*<i class/.test(ln)) misplaced.push(f + ':' + (i + 1) + ' textContent');
  });
}
check('no icon markup hides in a tooltip, a native dialog or a textContent write',
  misplaced.length === 0, misplaced.slice(0, 5).join(' | '));

// ── 5. Every icon referenced exists in the stylesheet ───────────────────────
const defined = new Set([...css.matchAll(/^\.ic-([a-z-]+)\{/gm)].map(m => m[1]));
const used = new Set();
for (const f of [...pages, ...scripts]) {
  for (const m of pub(f).matchAll(/class=["']ic ic-([a-z-]+)["']/g)) used.add(m[1]);
}
const undef = [...used].filter(n => !defined.has(n));
check('every icon a page asks for is defined — no silent blank squares',
  undef.length === 0 && used.size > 20, undef.join(', ') || (used.size + ' used'));
const MODIFIER = new Set(['dot', 'lg', 'sm']);   // .ic-lg / .ic-sm size the icon, they are not one
const unused = [...defined].filter(n => !used.has(n) && !MODIFIER.has(n));
check('the icon table carries no dead weight', unused.length === 0, unused.join(', '));

// ── 6. The nav rail is untouched ────────────────────────────────────────────
const side = pub('cygenix-sidebar.js');
check('the sidebar still draws its own icons inline, as the reference style',
  /class="cyg-nav-icon" viewBox="0 0 16 16"/.test(side)
  && (side.match(/stroke-width="1\.[123]"/g) || []).length > 20);

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
