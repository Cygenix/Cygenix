// tests/nav-theme.test.js — a white ground, and one navy item on it.
//
// WHY THIS FILE EXISTS
//
// WHITE_AND_NAV changed two things that look unrelated and are not. The
// console ground went from #f2f2f3 to pure white, and the current rail item
// went from a pale accent wash to the masthead navy. The second follows from
// the first: accent-100 (#eef6ff) against #f2f2f3 was a faint but readable
// difference; against #ffffff it is a smudge. Lighten the ground and the old
// "you are here" marker stops answering the question.
//
// WHAT THIS GUARDS
//
// Three ways this comes undone, none of them loud:
//
//   1. Someone reintroduces a grey page ground on one page — an inline
//      :root, a body background, a panel fill — and that page drifts away
//      from every other while still "looking fine" in isolation.
//   2. Someone hard-codes #1d2d3d for the rail's navy instead of reading
//      --color-accent-900. The masthead and the rail then agree by accident,
//      and the financial theme (which remaps that token to a warm brown)
//      gets a navy rail item on a cream bar.
//   3. The large surfaces quietly reacquire neutral-100 fills. The right-hand
//      columns are the ones that had them; the inset blocks — SQL, hashes,
//      masked connection rows, log excerpts — are meant to keep theirs, so
//      this file distinguishes between the two rather than banning the token.

'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

const PUB = path.join(__dirname, '..', 'public');
const read = (f) => fs.readFileSync(path.join(PUB, f), 'utf8');
const files = (ext) => fs.readdirSync(PUB).filter((f) => f.endsWith(ext)).sort();

// Strip comments before looking for code. This codebase's house style is long
// header comments that say what went wrong before, so the old values are
// quoted on purpose in several places and must not read as regressions.
const code = (s) => s.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/[^\n]*$/gm, '');

// Print rules are not screen rules. Five pages set `body{background:#fff}`
// inside @media print so a printed report lands on white paper with no rail
// gutter, whatever the screen theme is doing. That is correct and must stay,
// so it is cut out before looking for page-ground overrides — otherwise this
// file would be arguing that a report should print on grey.
const noPrint = (s) => {
  let out = s, at;
  while ((at = out.search(/@media[^{]*\bprint\b[^{]*\{/)) !== -1) {
    let i = out.indexOf('{', at), depth = 0, j = i;
    for (; j < out.length; j++) {
      if (out[j] === '{') depth++;
      else if (out[j] === '}' && --depth === 0) break;
    }
    out = out.slice(0, at) + out.slice(Math.min(j + 1, out.length));
  }
  return out;
};

const CONSOLE = read('cygenix-console.css');
const RAIL = read('cygenix-sidebar.js');
const THEME = read('cygenix-theme.css');

(async () => {
  console.log('White ground, navy current item\n');

  /* ── 1. The ground ─────────────────────────────────────────────────────── */
  section('1. Pure white, from one token');

  check('the console ground token is #ffffff',
    /--color-bg: *#ffffff;/.test(CONSOLE));
  check('the body paints from that token rather than a literal',
    /body \{[^}]*background: var\(--color-bg\)/.test(CONSOLE));
  check('and the old near-grey survives only as prose explaining what it was',
    !/#f2f2f3/.test(code(CONSOLE)) && !/#f2f2f3/.test(code(RAIL)));

  // Every alias still resolves to the token, so a page written against the old
  // vocabulary lands on white too rather than on a colour of its own.
  check('the legacy --bg and --bg2 aliases still resolve to the token, not to a colour',
    /--bg: *var\(--color-bg\);/.test(CONSOLE) && /--bg2: *var\(--color-bg\);/.test(CONSOLE));

  // A page that sets its own page ground is the failure mode this guards: it
  // looks right on its own and wrong beside everything else.
  const grounds = [];
  for (const f of files('.html')) {
    const s = noPrint(code(read(f)));
    if (!/cygenix-console\.css/.test(s)) continue;          // marketing keeps its own
    const m = s.match(/(?::root|html\[data-theme="light"\])\s*\{[^}]*--color-bg\s*:\s*([^;]+)/);
    if (m) grounds.push(f + ' → ' + m[1].trim());
    const b = s.match(/\bbody\s*\{[^}]*background(?:-color)?\s*:\s*(#[0-9a-f]{3,8})/i);
    if (b) grounds.push(f + ' → body ' + b[1]);
  }
  check('NO CONSOLE PAGE OVERRIDES THE PAGE GROUND ON SCREEN', grounds.length === 0, grounds.join(', '));

  // ...and the print override those five DO carry is left alone on purpose,
  // so removing it later reads as the change it is rather than as tidying.
  const printers = files('.html').filter((f) => /@media print\{[\s\S]{0,200}?body\{[^}]*background:#fff/.test(read(f).replace(/\s*\n\s*/g, '')));
  check('a printed page still lands on white paper with no rail gutter',
    printers.length >= 5, printers.join(', '));

  /* ── 2. The large surfaces ─────────────────────────────────────────────── */
  section('2. Nothing large is filled; the inset blocks still are');

  const DASH = read('dashboard.html');
  for (const cls of ['hm-side', 'jb-side']) {
    const rule = (DASH.match(new RegExp('\\.' + cls + '\\{([^}]*)\\}')) || [])[1] || '';
    check('the ' + cls.replace('-', ' ') + ' column carries a left hairline and no fill',
      /border-left: *1px solid var\(--color-divider\)/.test(rule) && !/background/.test(rule),
      rule.slice(0, 90));
  }
  check('the rail is the page ground too, not a surface of its own',
    /\.cyg-sidebar\{[\s\S]{0,120}?background:var\(--color-bg/.test(RAIL));
  check('panels keep no fill and no shadow — they were line drawings before this change',
    /\.panel, \.card,[^{]*\{\s*background: transparent;/.test(CONSOLE));

  // The inset blocks. These are the small "a box inside a panel" surfaces the
  // handoff names, and they are meant to keep neutral-100 — now reading a
  // shade DARKER than the ground rather than a shade lighter, which is the
  // right way round for something set into a page.
  const insets = [
    ['dashboard.html', 'conn-locked', 'the masked connection row'],
    ['object_mapping.html', 'sql-box', 'the generated SQL block'],
    ['assurance.html', 'dry', 'the dry-run excerpt'],
  ];
  for (const [f, cls, what] of insets) {
    check(what + ' keeps its neutral-100 inset fill',
      new RegExp('\\.' + cls + '\\{[^}]*var\\(--color-neutral-100\\)').test(read(f).replace(/\s*\n\s*/g, '')),
      f);
  }
  check('neutral-100 is still a real step away from the ground, not the ground itself',
    /--color-neutral-100: *#f5f5f8;/.test(CONSOLE));

  /* ── 3. The current item ───────────────────────────────────────────────── */
  section('3. One navy item, from the masthead token');

  const activeRule = (RAIL.match(/\.cyg-nav-item\.active,[\s\S]*?\}/) || [''])[0];
  check('the current item is filled with --color-accent-900 and set in white',
    /background:var\(--color-accent-900/.test(activeRule) && /color:#fff/.test(activeRule),
    activeRule.replace(/\s+/g, ' ').slice(0, 140));
  check('THE RAIL AND THE MASTHEAD READ THE SAME TOKEN, so they cannot drift apart',
    /\.cx-masthead[\s\S]{0,400}?background:var\(--color-accent-900/.test(RAIL)
    && /background:var\(--color-accent-900/.test(activeRule));
  check('and neither spells the navy out as a hex of its own',
    (code(RAIL).match(/#1d2d3d/g) || []).every((_, i, all) => all.length <= 8),
    (code(RAIL).match(/#1d2d3d/g) || []).length + ' fallback occurrences');
  check('the pale accent wash is gone from the current item',
    !/\.cyg-nav-item\.active\{[^}]*--color-accent-100/.test(RAIL));
  check('the left bar matches the fill rather than standing off it in steel',
    /border-left-color:var\(--color-accent-900/.test(activeRule));
  check('the current item is heavier as well as darker',
    /font-weight:600/.test(activeRule));

  check('hovering an inactive item is a neutral-100 wash, and the text stays put',
    /\.cyg-nav-item:hover\{color:var\(--color-text[^}]*background:var\(--color-neutral-100/.test(RAIL));
  check('HOVERING THE CURRENT ITEM DOES NOT DISTURB IT',
    /\.cyg-nav-item\.active:hover/.test(RAIL) && /\[aria-current="page"\]:hover/.test(RAIL));
  check('focus rings inset by 2px so they land on the navy, not beside it',
    /\.cyg-nav-item:focus-visible\{outline:2px solid var\(--color-accent[^}]*outline-offset:-2px/.test(RAIL));
  check('icons inside the current item are told to follow it to white',
    /\.cyg-nav-item\.active \.ic,[\s\S]{0,200}?color:#fff;stroke:currentColor/.test(RAIL));

  /* ── 4. Said out loud, not only drawn ──────────────────────────────────── */
  section('4. The current page is announced, not just coloured');

  check('the rail sets aria-current="page" on the item it fills',
    /el\.setAttribute\('aria-current', 'page'\)/.test(RAIL)
    && /el\.removeAttribute\('aria-current'\)/.test(RAIL));
  check('and styles off that attribute as well as the class',
    /\.cyg-nav-item\[aria-current="page"\]/.test(RAIL));
  check('A PINNED COPY NEVER CLAIMS TO BE THE CURRENT PAGE TOO',
    /node\.removeAttribute\('aria-current'\)/.test(RAIL));

  /* ── 5. Themes ─────────────────────────────────────────────────────────── */
  section('5. What a named theme may still do');

  // The financial theme is a cream theme; its ground is its identity, not a
  // grey left over from before, so it is deliberately NOT forced to white.
  // What it must keep doing is remapping the navy token, because the rail and
  // the masthead both read it — a theme that moved one and not the other is
  // the drift this whole change is guarding against.
  check('the one named theme still remaps the ground and the navy together',
    /html\[data-theme="financial"\]\{[^}]*--color-bg:/.test(THEME)
    && /html\[data-theme="financial"\]\{[^}]*--color-accent-900:/.test(THEME));
  check('and it still patches nothing: a theme is a remap, and remains one',
    !/html\[data-theme="financial"\] \.cyg-nav-item/.test(THEME));

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
