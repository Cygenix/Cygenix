// tests/mobile-css.test.js — the mobile layer, and the guarantee that it
// cannot touch desktop.
//
// The brief was "make it mobile friendly WITHOUT changing the desktop
// appearance". That is only trustworthy if it is structural: every
// declaration in cygenix-mobile.css must sit inside a max-width media
// query, so no rule can match a desktop viewport. This file proves that
// by parsing the stylesheet, and pins the mobile behaviours themselves.

'use strict';

const fs = require('fs');
const path = require('path');

const PUB = path.join(__dirname, '..', 'public');
const css = fs.readFileSync(path.join(PUB, 'cygenix-mobile.css'), 'utf8');
const sidebar = fs.readFileSync(path.join(PUB, 'cygenix-sidebar.js'), 'utf8');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Mobile layer — desktop immutability and mobile behaviour\n');

// ── The guarantee ─────────────────────────────────────────────────────────
// Strip comments, then walk the file tracking brace depth. Any selector
// block that opens at depth 0 without an enclosing @media is a rule that
// would apply at every width — exactly what must not exist here.
const noComments = css.replace(/\/\*[\s\S]*?\*\//g, '');
const strayRules = [];
{
  let depth = 0, inMedia = 0, buf = '';
  for (let i = 0; i < noComments.length; i++) {
    const ch = noComments[i];
    if (ch === '{') {
      const head = buf.trim();
      if (depth === 0) {
        if (/^@media\b/.test(head)) inMedia = 1;
        else if (head) strayRules.push(head.slice(0, 60));
      }
      depth++; buf = '';
    } else if (ch === '}') {
      depth--;
      if (depth === 0) inMedia = 0;
      buf = '';
    } else buf += ch;
  }
}
check('every rule is inside a media query — desktop cannot be reached',
  strayRules.length === 0, strayRules.join(' | '));

const mediaHeads = [...noComments.matchAll(/@media([^{]+)\{/g)].map(m => m[1].trim());
check('every media query is a max-width query (no min-width, no bare media)',
  mediaHeads.length > 0 && mediaHeads.every(h => /max-width:\s*\d+px/.test(h) && !/min-width/.test(h)),
  mediaHeads.join(' | '));
check('the breakpoints are the phone/small-tablet ones (820 and 480)',
  mediaHeads.some(h => /820px/.test(h)) && mediaHeads.some(h => /480px/.test(h)));

// ── Mobile behaviours ─────────────────────────────────────────────────────
check('the page reclaims the sidebar gutter',
  /body\s*\{[^}]*padding-left:\s*0\s*!important/.test(noComments)
  && /body\.cyg-collapsed\s*\{[^}]*padding-left:\s*0\s*!important/.test(noComments));
check('the sidebar becomes an off-canvas drawer that opens on a body class',
  /\.cyg-sidebar\s*\{[^}]*transform:\s*translateX\(-100%\)/.test(noComments)
  && /body\.cyg-mobile-open\s+\.cyg-sidebar\s*\{[^}]*translateX\(0\)/.test(noComments));
check('the drawer never renders as the 54px icon rail on a phone',
  /\.cyg-sidebar\.collapsed\s*\{[^}]*width:\s*268px\s*!important/.test(noComments));
check('a hamburger and a backdrop are styled for the drawer',
  /\.cyg-mobile-menu-btn\s*\{/.test(noComments) && /\.cyg-mobile-backdrop\s*\{/.test(noComments));
check('multi-column grids collapse — including the inline ones',
  /\[style\*="grid-template-columns"\]\s*\{[^}]*grid-template-columns:\s*1fr\s*!important/.test(noComments)
  && /\.stats-row[\s\S]{0,120}grid-template-columns:\s*1fr\s*!important/.test(noComments));
check('the split workspaces (Execute, SQL Editor, Object Mapping) stack',
  /\.layout\s*\{[^}]*grid-template-columns:\s*1fr\s*!important/.test(noComments)
  && /\.app\s*\{[^}]*grid-template-columns:\s*1fr\s*!important/.test(noComments));
check('data tables scroll inside their own box rather than stretching the page',
  /(^|\n)\s*table\s*\{[^}]*overflow-x:\s*auto/.test(noComments));
check('form controls render at 16px, which is what stops iOS zooming on focus',
  /input,\s*select,\s*textarea\s*\{[^}]*font-size:\s*16px\s*!important/.test(noComments));
check('tap targets reach 40px',
  /min-height:\s*40px/.test(noComments));
check('tab bars scroll sideways instead of wrapping into a pile',
  /\.cx-tabs[\s\S]{0,200}overflow-x:\s*auto\s*!important/.test(noComments));
check('drawers and modals go full-width',
  /#lineage-drawer\s*\{[^}]*width:\s*100vw\s*!important/.test(noComments)
  && /\.modal[\s\S]{0,160}max-width:\s*96vw\s*!important/.test(noComments));
check('canvas views get a workable portrait height',
  /#se-view-atlas[\s\S]{0,200}min-height:\s*60vh/.test(noComments));

// ── Sidebar drawer wiring ─────────────────────────────────────────────────
check('the drawer is wired on mount, behind a matchMedia gate',
  /wireMobileDrawer\(aside\)/.test(sidebar)
  && /const MOBILE_Q = '\(max-width: 820px\)'/.test(sidebar)
  && /matchMedia\(MOBILE_Q\)/.test(sidebar));
check('leaving mobile width clears the drawer state, so desktop is never left half-open',
  /if \(!mq\.matches\) setOpen\(false\)/.test(sidebar));
check('the drawer closes on backdrop click, Escape, and following a link',
  /backdrop\.addEventListener\('click'/.test(sidebar)
  && /e\.key === 'Escape'/.test(sidebar)
  && /a\[href\]'\)\) setOpen\(false\)/.test(sidebar));
check('the hamburger carries its accessible name and state',
  /aria-label', 'Open menu'/.test(sidebar) && /aria-expanded', String\(open\)/.test(sidebar));

// ── Every page opts in ────────────────────────────────────────────────────
const pages = fs.readdirSync(PUB).filter(f => f.endsWith('.html'));
const missingCss = pages.filter(f => !fs.readFileSync(path.join(PUB, f), 'utf8').includes('cygenix-mobile.css'));
check('every page (' + pages.length + ') links the mobile stylesheet', missingCss.length === 0, missingCss.join(', '));
const missingViewport = pages.filter(f => !/name="viewport"/i.test(fs.readFileSync(path.join(PUB, f), 'utf8')));
check('every page declares a viewport meta', missingViewport.length === 0, missingViewport.join(', '));
const badViewport = pages.filter(f => /user-scalable\s*=\s*no|maximum-scale\s*=\s*1/i.test(fs.readFileSync(path.join(PUB, f), 'utf8')));
check('no page blocks pinch-zoom (accessibility)', badViewport.length === 0, badViewport.join(', '));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
