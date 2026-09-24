// tests/typography.test.js — one face, sentence case, no registration marks.
//
// WHY THIS FILE EXISTS
//
// The v2 handoff (TYPE_AND_FRAMES) changed three things at once, and it had
// to: they are the same decision seen from three sides.
//
// Barlow Condensed is a CONDENSED DISPLAY FACE. The design it arrived with
// leaned on that — small uppercase labels with wide tracking, which a
// condensed face carries comfortably. Noto Sans is a normal-width text face.
// Set the same labels in it and they shout and run wide: a 13px label at
// .16em tracking costs half a row more per label, and a 40px uppercase title
// stops fitting at 1280px. So swapping only the family would have produced a
// worse screen than either design. The face, the case and the tracking move
// together or not at all.
//
// The corner marks went for the same reason. Four crosshairs at the corners
// of a panel read as drafting next to a condensed face and as decoration next
// to a text face.
//
// WHAT THIS GUARDS
//
// Regression here is silent and cumulative. One page adds `font-family:
// 'Helvetica Neue'` to an inline style, one component keeps its uppercase,
// and six months later the console is set in three faces again — which is
// exactly the drift the shared token block was created to stop.

'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

const ROOT = path.join(__dirname, '..');
const PUB = path.join(ROOT, 'public');
const read = (f) => fs.readFileSync(path.join(PUB, f), 'utf8');
const files = (ext) => fs.readdirSync(PUB).filter((f) => f.endsWith(ext)).sort();

// The console pages are the ones the handoff covers: those that load the
// console stylesheet. The older screens (connect, report, admin, job-editor
// and friends) still use the previous --text3/--bg3 vocabulary and have not
// been converted; converting them is a larger job than typography and is
// tracked separately. Marketing keeps its own hero language by design.
const CONSOLE_PAGES = files('.html').filter((f) => /cygenix-console\.css/.test(read(f)));

// Files that render INSIDE a console page, so their rules are console rules
// wherever they live.
const CONSOLE_CSS = ['cygenix-console.css', 'cygenix-brand.css', 'cygenix-datastream.css', 'cygenix-history.css'];

(async () => {
  console.log('Typography — Noto Sans, sentence case, no corner marks\n');

  /* ── 1. One face ───────────────────────────────────────────────────── */
  section('1. Noto Sans, and only Noto Sans');

  const CONSOLE = read('cygenix-console.css');
  const BRAND = read('cygenix-brand.css');

  check('the console stylesheet declares Noto Sans as both heading and body',
    /--font-heading: *'Noto Sans'/.test(CONSOLE) && /--font-body: *'Noto Sans'/.test(CONSOLE));
  check('the public pages resolve to the same family',
    /--sans: *'Noto Sans'/.test(BRAND) && /--serif: *'Noto Sans'/.test(BRAND));
  check('four weights are self-hosted, not fetched from a third party',
    [400, 500, 600, 700].every((w) => new RegExp("font-weight: " + w + ";[^}]*noto-sans-" + w + "-latin\\.woff2").test(CONSOLE)));
  check('the woff2 files are actually present and are woff2',
    [400, 500, 600, 700].every((w) => {
      const p = path.join(PUB, 'fonts', 'noto-sans-' + w + '-latin.woff2');
      return fs.existsSync(p) && fs.readFileSync(p).slice(0, 4).toString('latin1') === 'wOF2';
    }));
  check('every face declares font-display: swap, so text paints before the font arrives',
    (CONSOLE.match(/font-display: swap/g) || []).length >= 4);
  check('no page links a font stylesheet from a third party',
    !files('.html').some((f) => /fonts\.googleapis\.com\/css/.test(read(f))
      && /cygenix-console\.css/.test(read(f))));

  // The old faces, gone. The console stylesheet's own prose may name them:
  // it explains what it replaced and why, which is the house style.
  const oldFace = [];
  for (const ext of ['.html', '.js', '.css']) {
    for (const f of files(ext)) {
      const s = read(f);
      const code = s.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/[^\n]*$/gm, '');
      if (/IBM Plex Sans|Barlow/.test(code)) oldFace.push(f);
    }
  }
  check('NO PAGE STILL NAMES IBM Plex Sans OR Barlow in its code', oldFace.length === 0, oldFace.join(', '));
  check('and the Barlow woff2 files are gone from /fonts',
    fs.readdirSync(path.join(PUB, 'fonts')).every((f) => !/barlow/i.test(f)),
    fs.readdirSync(path.join(PUB, 'fonts')).join(', '));
  check('the monospace face is untouched — code wants a monospace and always did',
    /--mono: *'IBM Plex Mono'/.test(BRAND) && /--mono: *'IBM Plex Mono'/.test(CONSOLE));
  check('and it is self-hosted too, so no console page fetches a font from a third party',
    [400, 500].every((w) => new RegExp("ibm-plex-mono-" + w + "-latin\\.woff2").test(CONSOLE)
      && fs.existsSync(path.join(PUB, 'fonts', 'ibm-plex-mono-' + w + '-latin.woff2'))));

  /* ── 2. Sentence case ──────────────────────────────────────────────── */
  section('2. Sentence case, and no tracking bought for capitals');

  const shouting = [];
  for (const f of CONSOLE_PAGES.concat(CONSOLE_CSS)) {
    const s = read(f);
    s.split('\n').forEach((line, i) => {
      if (/text-transform: *uppercase/.test(line)) shouting.push(f + ':' + (i + 1));
    });
  }
  check('NO CONSOLE PAGE OR SHARED STYLESHEET UPPERCASES ANYTHING', shouting.length === 0, shouting.join(' '));

  // The environment tag still reads PROD, but as data rather than as a
  // transform: the value arrives capitalised because it is a code.
  check('the environment tag still reads in capitals, from the data',
    /id="src-env-tag"[^>]*>PROD</.test(read('dashboard.html')));
  check('and the tag class no longer transforms, so a sentence sharing it is not shouted',
    /\.cx-tag \{[^}]*letter-spacing: 0;/.test(CONSOLE) && !/\.cx-tag \{[^}]*uppercase/.test(CONSOLE));

  const tracked = [];
  for (const f of CONSOLE_CSS) {
    read(f).split('\n').forEach((line, i) => {
      // Tracking of .06em and up was bought to make capitals legible. The
      // wordmark keeps its own, being a brand mark rather than a label.
      if (/letter-spacing: *\.?0?\.(0[6-9]|1[0-9]|2[0-9])em/.test(line)) tracked.push(f + ':' + (i + 1));
    });
  }
  check('no uppercase-era tracking survives in the shared stylesheets', tracked.length === 0, tracked.join(' '));

  /* ── 3. The type scale ─────────────────────────────────────────────── */
  section('3. The scale from the handoff');

  const rule = (sel) => {
    const m = CONSOLE.match(new RegExp('\\' + sel + ' \\{([^}]*)\\}'));
    return m ? m[1] : '';
  };
  check('page title 34px / 600 / 1.15, and it wraps rather than truncating',
    /font-size: 34px/.test(rule('.cx-title')) && /line-height: 1\.15/.test(rule('.cx-title')) && /text-wrap: balance/.test(rule('.cx-title')));
  check('the project title on Home is 40px', /font-size: 40px/.test(CONSOLE.match(/\.cx-title\.cx-title-project \{([^}]*)\}/)[1]));
  check('panel heading 22px / 1.25, sub-panel 17px / 1.3',
    /font-size: 22px; line-height: 1\.25/.test(rule('.cx-h')) && /font-size: 17px; line-height: 1\.3/.test(rule('.cx-h-sm')));
  check('section label 15px / 1.3 / neutral-700', /font-size: 15px; line-height: 1\.3/.test(rule('.cx-section')));
  check('kicker 13px / 1.3 / accent-700', /font-size: 13px; line-height: 1\.3/.test(rule('.cx-kicker')));
  check('tabs are 15px and gain their weight when active, not their case',
    /font-size: 15px/.test(rule('.tab')) && /font-weight: 500/.test(rule('.tab'))
    && /font-weight: 600/.test(CONSOLE.match(/\.tab\.active \{([^}]*)\}/)[1]));
  check('table headers are 12px / 600 — nothing on the screen is below 12px',
    /font-size: 12px/.test(rule('.cx-table th')) && /font-weight: 600/.test(rule('.cx-table th')));
  check('NOTHING IN THE SHARED STYLESHEET SETS TYPE BELOW 12px', (() => {
    const small = [];
    CONSOLE.split('\n').forEach((line, i) => {
      const m = line.match(/font-size: *(\d+(?:\.\d+)?)px/);
      if (m && parseFloat(m[1]) < 12) small.push((i + 1) + ': ' + line.trim().slice(0, 60));
    });
    return small.length === 0 || small.join(' | ');
  })() === true, (() => {
    const small = [];
    CONSOLE.split('\n').forEach((line, i) => {
      const m = line.match(/font-size: *(\d+(?:\.\d+)?)px/);
      if (m && parseFloat(m[1]) < 12) small.push((i + 1) + ':' + m[1] + 'px');
    });
    return small.join(' ');
  })());
  check('measure labels are 13px and numerals are tabular',
    /font-size: 13px/.test(rule('.cx-measure-label')) && /tabular-nums/.test(CONSOLE));

  /* ── 4. No corner marks ────────────────────────────────────────────── */
  section('4. The registration marks are gone');

  const marks = [];
  for (const ext of ['.html', '.js', '.css']) {
    for (const f of files(ext)) if (/cx-corner|class="corner/.test(read(f))) marks.push(f);
  }
  check('NO PAGE STILL INJECTS A CORNER MARK', marks.length === 0, marks.join(', '));
  check('and the stylesheet has no crosshair rules left',
    !/\.cx-corner/.test(CONSOLE) && !/\.cx-blueprint *> */.test(CONSOLE));
  check('the panel itself survives: a hairline, square corners, no fill',
    /\.cx-blueprint \{ position: relative; border: 1px solid var\(--color-divider\); background: transparent; \}/.test(CONSOLE));
  check('the panels that used the marks still exist, now distinguished by position',
    /cx-blueprint/.test(read('dashboard.html')) && /cx-blueprint/.test(read('assurance.html')));

  /* ── 5. What was deliberately left ─────────────────────────────────── */
  section('5. The boundary of this change, written down');

  // Marketing keeps its own hero language by design — the handoff is a
  // CONSOLE redesign and says so in its title.
  const MARKETING = ['index.html', 'pricing.html', 'about.html', 'help.html',
                     'register.html', 'privacy.html', 'terms.html'];
  const legacy = files('.html')
    .filter((f) => !CONSOLE_PAGES.includes(f) && !MARKETING.includes(f) && /text-transform: *uppercase/.test(read(f)));
  // These eleven still use the previous --text3/--bg3 vocabulary. Converting
  // them is a larger job than typography and is deliberately not in this
  // change; the count is pinned so the set cannot grow unnoticed.
  check('the pages left on the old vocabulary are a known, listed set — not a surprise',
    legacy.length > 0 && legacy.length <= 11, legacy.length + ': ' + legacy.join(', '));
  check('none of them is a screen the handoff specifies',
    !legacy.some((f) => ['dashboard.html', 'assurance.html', 'object_mapping.html', 'audit.html'].includes(f)),
    legacy.join(', '));
  check('every console page is covered, and there are as many as the console has',
    CONSOLE_PAGES.length >= 28, CONSOLE_PAGES.length + ' pages');

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
