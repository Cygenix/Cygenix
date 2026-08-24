#!/usr/bin/env node
/* ============================================================================
   build-icons.js — generates public/cygenix-icons.css from the table below.
   ----------------------------------------------------------------------------
   One visual language for the whole console. The sidebar draws its nav icons
   as inline SVG (cygenix-sidebar.js); pages need the same artwork inside
   buttons, panel titles and status chips, where the markup is built as
   strings and an inline <svg> would be unreadable and quote-fragile.

   So the same drawings ship as CSS mask images: `<i class="ic ic-save"></i>`.
   A mask takes only the alpha channel, so `background: currentColor` paints
   the icon in whatever colour the surrounding text is — exactly how the nav
   icons behave, and why a red button and a muted toolbar get the right icon
   without a second copy of the artwork.

   House rules, matched to the sidebar: 16×16 grid, stroke currentColor,
   stroke-width 1.2 (1.3 for a lone tick or arrow that would look thin),
   round caps and joins, fill only where a shape is deliberately solid.

   Run: node scripts/build-icons.js   (then node scripts/stamp-assets.js)
   ========================================================================== */
'use strict';
const fs = require('fs');
const path = require('path');

const S = 'stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"';
const S13 = 'stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"';

/* name → SVG body. Keep alphabetical-ish by theme; every entry is used. */
const ICONS = {
  // ── files and documents ──
  save:      `<path d="M3 2h8l2 2v10H3z" ${S}/><path d="M5.5 2v4h5V2M5 9.5h6v4.5H5z" ${S}/>`,
  file:      `<path d="M4 2h5l3 3v9H4z" ${S}/><path d="M9 2v3h3" ${S}/>`,
  folder:    `<path d="M2 4.5h4l1.2 1.5H14v6.5a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1z" ${S}/>`,
  folderOpen:`<path d="M2 12.5V4.5h4l1.2 1.5H13v2" ${S}/><path d="M2.6 12.5 4.4 8h10l-1.8 4.5z" ${S}/>`,
  doc:       `<path d="M4 2h5l3 3v9H4z" ${S}/><path d="M6 7.5h4M6 10h4" ${S}/>`,
  book:      `<path d="M3 3h4a1.6 1.6 0 0 1 1 1.4V13a1.4 1.4 0 0 0-1-1H3z" ${S}/><path d="M13 3H9a1.6 1.6 0 0 0-1 1.4V13a1.4 1.4 0 0 1 1-1h4z" ${S}/>`,
  clipboard: `<rect x="3.5" y="3" width="9" height="11" rx="1" ${S}/><rect x="6" y="1.6" width="4" height="2.6" rx="0.5" ${S}/>`,
  package:   `<path d="M8 2 14 5v6l-6 3-6-3V5z" ${S}/><path d="M2 5l6 3 6-3M8 8v6" ${S}/>`,
  archive:   `<rect x="2" y="3" width="12" height="3" rx="0.6" ${S}/><path d="M3 6v7.5h10V6" ${S}/><path d="M6.5 9h3" ${S}/>`,
  database:  `<ellipse cx="8" cy="4" rx="5" ry="2" ${S}/><path d="M3 4v8c0 1.1 2.2 2 5 2s5-.9 5-2V4" ${S}/><path d="M3 8c0 1.1 2.2 2 5 2s5-.9 5-2" ${S}/>`,
  // ── actions ──
  search:    `<circle cx="7" cy="7" r="4.2" ${S}/><path d="M10.2 10.2 14 14" ${S}/>`,
  eye:       `<path d="M1.5 8S4 3.5 8 3.5 14.5 8 14.5 8 12 12.5 8 12.5 1.5 8 1.5 8Z" ${S}/><circle cx="8" cy="8" r="1.8" ${S}/>`,
  download:  `<path d="M8 2v8m0 0L5 7m3 3 3-3M3 13.5h10" ${S}/>`,
  upload:    `<path d="M8 11V3m0 0L5 6m3-3 3 3M3 13.5h10" ${S}/>`,
  sync:      `<path d="M2.8 6.6A5 5 0 0 1 12 5.4" ${S}/><path d="M12.4 2.8v2.8H9.6" ${S}/><path d="M13.2 9.4A5 5 0 0 1 4 10.6" ${S}/><path d="M3.6 13.2v-2.8h2.8" ${S}/>`,
  undo:      `<path d="M3 8a5 5 0 1 0 1.6-3.7" ${S}/><path d="M2.8 2.4v2.8h2.8" ${S}/>`,
  trash:     `<path d="M3 4.5h10M6.5 4.5V3h3v1.5" ${S}/><path d="M4.3 4.5 5 13.2a.8.8 0 0 0 .8.8h4.4a.8.8 0 0 0 .8-.8l.7-8.7" ${S}/><path d="M6.8 7v4.5M9.2 7v4.5" ${S}/>`,
  edit:      `<path d="M11.2 2.6 13.4 4.8 5.6 12.6 2.6 13.4l.8-3z" ${S}/>`,
  plus:      `<path d="M8 3v10M3 8h10" ${S13}/>`,
  check:     `<path d="M3 8.4 6.3 11.7 13 5" ${S13}/>`,
  print:     `<path d="M4.5 6V2.5h7V6" ${S}/><rect x="2.5" y="6" width="11" height="5" rx="1" ${S}/><path d="M4.5 9.5h7v4h-7z" ${S}/>`,
  fullscreen:`<path d="M2.5 6V2.5H6M10 2.5h3.5V6M13.5 10v3.5H10M6 13.5H2.5V10" ${S}/>`,
  stop:      `<rect x="4" y="4" width="8" height="8" rx="1" ${S}/>`,
  pause:     `<path d="M6 3.5v9M10 3.5v9" ${S13}/>`,
  skip:      `<path d="M4 3.5 10 8l-6 4.5z" ${S}/><path d="M12 3.5v9" ${S}/>`,
  // ── objects and concepts ──
  robot:     `<rect x="2.5" y="5" width="11" height="8" rx="1.6" ${S}/><path d="M8 2.2V5" ${S}/><circle cx="8" cy="1.9" r="0.9" ${S}/><circle cx="5.8" cy="8.6" r="0.85" fill="currentColor"/><circle cx="10.2" cy="8.6" r="0.85" fill="currentColor"/>`,
  sparkle:   `<path d="M6 2.2 7.1 5.4 10.3 6.5 7.1 7.6 6 10.8 4.9 7.6 1.7 6.5 4.9 5.4z" ${S}/><path d="M11.8 9.2l.6 1.7 1.7.6-1.7.6-.6 1.7-.6-1.7-1.7-.6 1.7-.6z" ${S}/>`,
  wand:      `<path d="M3 13 10.5 5.5" ${S}/><path d="M12.4 3.6 13.6 4.8 11.7 6.7 10.5 5.5z" ${S}/><path d="M5.5 2.5v2M4.5 3.5h2M12 9v1.8M11.1 9.9h1.8" ${S}/>`,
  plug:      `<path d="M6 2v3.5M10 2v3.5" ${S}/><path d="M4 5.5h8v2a4 4 0 0 1-8 0z" ${S}/><path d="M8 11.5V14" ${S}/>`,
  chart:     `<path d="M2.5 13.5h11" ${S}/><path d="M4.5 13.5V8M8 13.5V4M11.5 13.5v-3.5" ${S}/>`,
  clock:     `<circle cx="8" cy="8" r="5.5" ${S}/><path d="M8 4.8V8l2.2 1.6" ${S}/>`,
  hourglass: `<path d="M4.5 2.5h7M4.5 13.5h7" ${S}/><path d="M5 2.5c0 3 3 4 3 5.5s-3 2.5-3 5.5M11 2.5c0 3-3 4-3 5.5s3 2.5 3 5.5" ${S}/>`,
  calendar:  `<rect x="2.5" y="3.5" width="11" height="10" rx="1" ${S}/><path d="M2.5 6.5h11M5.5 2v3M10.5 2v3" ${S}/>`,
  lock:      `<rect x="3.5" y="7" width="9" height="6.5" rx="1" ${S}/><path d="M5.6 7V5.2a2.4 2.4 0 0 1 4.8 0V7" ${S}/>`,
  key:       `<circle cx="5" cy="6" r="2.8" ${S}/><path d="M7 8l5 5M10 11l1.3-1.3M11.5 12.5l1.3-1.3" ${S}/>`,
  shield:    `<path d="M8 2 3 4v4c0 3 2 5 5 6 3-1 5-3 5-6V4z" ${S}/>`,
  link:      `<path d="M6.6 9.4a2.6 2.6 0 0 1 0-3.6l2-2a2.55 2.55 0 0 1 3.6 3.6l-1 1" ${S}/><path d="M9.4 6.6a2.6 2.6 0 0 1 0 3.6l-2 2a2.55 2.55 0 0 1-3.6-3.6l1-1" ${S}/>`,
  chain:     `<path d="M6.6 9.4a2.6 2.6 0 0 1 0-3.6l2-2a2.55 2.55 0 0 1 3.6 3.6l-1 1" ${S}/><path d="M9.4 6.6a2.6 2.6 0 0 1 0 3.6l-2 2a2.55 2.55 0 0 1-3.6-3.6l1-1" ${S}/>`,
  cloud:     `<path d="M4.6 12.5a3 3 0 0 1-.3-6 4 4 0 0 1 7.7.9 2.6 2.6 0 0 1-.4 5.1z" ${S}/>`,
  bolt:      `<path d="M9 1.8 4 9h3.4l-.4 5.2L12 7H8.6z" ${S}/>`,
  target:    `<circle cx="8" cy="8" r="5.5" ${S}/><circle cx="8" cy="8" r="2.6" ${S}/><circle cx="8" cy="8" r="0.9" fill="currentColor"/>`,
  map:       `<path d="M6 3 2.5 4.4v9L6 12.1l4 1.4 3.5-1.4v-9L10 4.4z" ${S}/><path d="M6 3v9.1M10 4.4v9.1" ${S}/>`,
  compass:   `<circle cx="8" cy="8" r="5.8" ${S}/><path d="M10.4 5.6 9.2 9.2 5.6 10.4 6.8 6.8z" ${S}/>`,
  /* sliders, not a gear: the nav rail's Settings mark is sliders, and at 12px
     a ringed gear reads as a sunburst */
  cog:       `<path d="M2 4.6h7M12.6 4.6h1.4M2 11.4h1.4M7 11.4h7" ${S}/><circle cx="10.6" cy="4.6" r="1.7" ${S}/><circle cx="5" cy="11.4" r="1.7" ${S}/>`,
  tools:     `<path d="M9.6 4.4a2.8 2.8 0 0 0 3.7 3.6l-6 6a1.6 1.6 0 0 1-2.3-2.3z" ${S}/><path d="M4.5 2.2 2.2 4.5l2.6 2.6 2.3-2.3z" ${S}/>`,
  flask:     `<path d="M6.5 2v4.2L3.1 12a1.2 1.2 0 0 0 1 1.9h7.8a1.2 1.2 0 0 0 1-1.9L9.5 6.2V2" ${S}/><path d="M5.5 2h5M4.8 9.6h6.4" ${S}/>`,
  helix:     `<path d="M5 2c0 4 6 4 6 8M11 2c0 4-6 4-6 8" ${S}/><path d="M5.4 4.5h5.2M4.8 8h6.4M5.4 11.5h5.2" ${S}/>`,
  user:      `<circle cx="8" cy="5.4" r="2.6" ${S}/><path d="M3 13.4a5 5 0 0 1 10 0" ${S}/>`,
  users:     `<circle cx="6" cy="5.4" r="2.4" ${S}/><path d="M1.8 13.2a4.4 4.4 0 0 1 8.4 0" ${S}/><path d="M10.6 3.4a2.4 2.4 0 0 1 0 4M11.8 9.2a4.4 4.4 0 0 1 2.4 4" ${S}/>`,
  mail:      `<rect x="2" y="3.5" width="12" height="9" rx="1" ${S}/><path d="M2.4 4.4 8 8.6l5.6-4.2" ${S}/>`,
  chat:      `<path d="M13.5 8.6c0 2.5-2.5 4.5-5.5 4.5a6.7 6.7 0 0 1-1.9-.3l-3.6 1 1.1-2.7A4.3 4.3 0 0 1 2.5 8.6C2.5 6.1 5 4 8 4s5.5 2.1 5.5 4.6Z" ${S}/>`,
  bell:      `<path d="M8 2.2a3.6 3.6 0 0 0-3.6 3.6c0 2.6-.9 3.6-1.4 4.1a.5.5 0 0 0 .35.85h9.3a.5.5 0 0 0 .35-.85c-.5-.5-1.4-1.5-1.4-4.1A3.6 3.6 0 0 0 8 2.2Z" ${S}/><path d="M6.6 13a1.5 1.5 0 0 0 2.8 0" ${S}/>`,
  pin:       `<path d="M8 14V9.6" ${S}/><path d="M5 2.5h6l-.9 3.2 1.6 2.2H4.3l1.6-2.2z" ${S}/>`,
  tag:       `<path d="M2.5 7.4V2.5h4.9l6.1 6.1-4.9 4.9z" ${S}/><circle cx="5.2" cy="5.2" r="1" fill="currentColor"/>`,
  rocket:    `<path d="M6.4 9.6C5 8.2 6.2 4.4 9 2.6c1.6-1 3.4-.8 4.4.2s1.2 2.8.2 4.4c-1.8 2.8-5.6 4-7.2 2.4z" ${S}/><path d="M6.4 9.6 4 12M5.2 8.4 2.6 9.2l1 1M7.6 10.8l-.8 2.6 1-1" ${S}/>`,
  truck:     `<path d="M1.5 4.5h8v7h-8z" ${S}/><path d="M9.5 7h2.6l2 2.4v2.1h-4.6z" ${S}/><circle cx="4.6" cy="12.4" r="1.4" ${S}/><circle cx="11.4" cy="12.4" r="1.4" ${S}/>`,
  scissors:  `<circle cx="4" cy="12" r="1.8" ${S}/><circle cx="12" cy="12" r="1.8" ${S}/><path d="M5.3 10.7 12 2.5M10.7 10.7 4 2.5" ${S}/>`,
  broom:     `<path d="M10.5 2.5 6 7" ${S}/><path d="M4.4 7.4 8.6 11.6l-2.2 1.9H4.2l-1.7-1.7v-2.2z" ${S}/><path d="M6 9.6 4.4 11.2" ${S}/>`,
  scale:     `<path d="M8 2.5v11M4 4.5h8" ${S}/><path d="M3 11a2.5 2.5 0 0 0 4 0L5 6z" ${S}/><path d="M9 11a2.5 2.5 0 0 0 4 0L11 6z" ${S}/>`,
  bucket:    `<path d="M2.5 5h11l-1.2 8.2a.8.8 0 0 1-.8.7H4.5a.8.8 0 0 1-.8-.7z" ${S}/><path d="M4.6 5a3.4 3.4 0 0 1 6.8 0" ${S}/>`,
  monitor:   `<rect x="2" y="3" width="12" height="8" rx="1" ${S}/><path d="M6 13.5h4M8 11v2.5" ${S}/>`,
  factory:   `<path d="M2.5 13.5V7l4 2.4V7l4 2.4V3.5h3v10z" ${S}/>`,
  menu:      `<path d="M2.5 4.5h11M2.5 8h11M2.5 11.5h11" ${S}/>`,
  takeoff:   `<path d="M2 13.5h12" ${S}/><path d="M3.4 9.6l1.7.5 2.4-2.1-3.2-3.4 1.3-.4 4.2 2.7 2.3-2a1.2 1.2 0 0 1 1.7 1.7l-8.6 5.6-1.8-2.6z" ${S}/>`,

  // ── status ──
  warning:   `<path d="M8 2.4 14.2 13H1.8z" ${S}/><path d="M8 6.6v3.1" ${S}/><circle cx="8" cy="11.4" r="0.75" fill="currentColor"/>`,
  info:      `<circle cx="8" cy="8" r="5.8" ${S}/><path d="M8 7.4v3.4" ${S}/><circle cx="8" cy="5.3" r="0.75" fill="currentColor"/>`,
  checkCircle:`<circle cx="8" cy="8" r="5.8" ${S}/><path d="M5.4 8.2 7.2 10 10.7 6" ${S}/>`,
  blocked:   `<circle cx="8" cy="8" r="5.8" ${S}/><path d="M4.1 11.9 11.9 4.1" ${S}/>`,
};

const body = (b) => '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16" fill="none">' + b + '</svg>';
const uri = (b) => 'data:image/svg+xml,' + encodeURIComponent(body(b))
  .replace(/'/g, '%27').replace(/\(/g, '%28').replace(/\)/g, '%29');

const rules = Object.keys(ICONS).map(k =>
  '.ic-' + k.replace(/[A-Z]/g, m => '-' + m.toLowerCase())
  + '{-webkit-mask-image:url("' + uri(ICONS[k]) + '");mask-image:url("' + uri(ICONS[k]) + '")}');

const css = `/* cygenix-icons.css — GENERATED by scripts/build-icons.js. Do not edit by
   hand: change the icon table in that script and re-run it, or the next build
   silently reverts your edit.

   One icon language for the console. The nav rail draws its icons as inline
   SVG; everywhere else uses <i class="ic ic-NAME"></i>, the same artwork as a
   mask so it takes the colour of whatever text it sits in — a red button, a
   muted toolbar, a green status line — from one copy of the drawing.

   Sizing follows the text: 1em square, nudged onto the baseline. .ic-lg for
   page titles and empty states, .ic-sm where a chip is tight. */
.ic{display:inline-block;width:1em;height:1em;vertical-align:-0.14em;flex:none;
  background-color:currentColor;
  -webkit-mask-repeat:no-repeat;mask-repeat:no-repeat;
  -webkit-mask-position:center;mask-position:center;
  -webkit-mask-size:contain;mask-size:contain}
.ic-lg{width:1.45em;height:1.45em;vertical-align:-0.28em}
.ic-sm{width:0.85em;height:0.85em}
/* A status dot is not an icon: it is a colour, and it should read as one at
   any size rather than as a coloured emoji that changes shape per platform. */
.ic-dot{display:inline-block;width:0.6em;height:0.6em;border-radius:50%;
  background:currentColor;vertical-align:0.04em;flex:none}

${rules.join('\n')}
`;

const out = path.join(__dirname, '..', 'public', 'cygenix-icons.css');
fs.writeFileSync(out, css);
console.log('build-icons: ' + Object.keys(ICONS).length + ' icons → public/cygenix-icons.css ('
  + Math.round(css.length / 1024) + ' KB)');
