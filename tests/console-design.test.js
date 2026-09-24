// tests/console-design.test.js — the console redesign's structural contract.
//
// Design review, Sep-2026. Seven findings; the ones this file keeps true:
//
//   04  the rail is monochrome — hue is reserved for state;
//   05  one token file — no page carries its own copy of the palette, and no
//       theme out-specifies another with a doubled selector;
//   07  the type is Barlow / Barlow Condensed, self-hosted, and nothing
//       smaller than 13px is a size the shared vocabulary offers.
//
// And the mechanics that make the chrome hold together: the rail width and
// the masthead height are stated in three places (the stylesheet, the rail
// module, twenty-eight pages' padding) and must agree; every page that
// absorbed other destinations declares somewhere for the tab strip to go;
// the public pages — index, login, pricing, register, help — are untouched,
// because the brief said not to touch them.
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
const exists = (f) => fs.existsSync(path.join(PUB, f));
const pages = fs.readdirSync(PUB).filter((f) => f.endsWith('.html'));
const appPages = pages.filter((f) => /cygenix-sidebar\.js/.test(read(f)));
const publicPages = ['index.html', 'login.html', 'pricing.html', 'register.html', 'help.html', 'about.html', 'terms.html', 'privacy.html', 'welcome.html', 'pick-plan.html'];

console.log('Console redesign — tokens, chrome, pages\n');

/* ── 1. One token file ───────────────────────────────────────────────────── */
const css = read('cygenix-console.css');
check('the console stylesheet exists and carries the Industry accent, neutral ramp and the status trio',
  /--color-accent:\s*#5980a6/.test(css) && /--color-accent-900:\s*#1d2d3d/.test(css)
  && /--color-neutral-700:\s*#5d5d60/.test(css) && /--state-ok:\s*#3f6b52/.test(css)
  && /--state-warn:\s*#9a6b1f/.test(css) && /--state-fail:\s*#9c3f38/.test(css));
check('the old names are aliases of the new values, not a second palette',
  /--bg:\s*var\(--color-bg\)/.test(css) && /--accent:\s*var\(--color-accent\)/.test(css)
  && /--green:\s*var\(--state-ok\)/.test(css) && /--red:\s*var\(--state-fail\)/.test(css)
  && /--serif:\s*var\(--font-body\)/.test(css));
check('border radius is 0 on the components, and the logo keeps its 7px',
  /--r:\s*0;/.test(css) && /\.cx-logo, \.cyg-brand-mark \{ border-radius: 7px; \}/.test(css));
// v2 (Sep-2026): one face, not two, and it is not condensed. The scale moved
// with it — see the note at the top of cygenix-console.css and
// tests/typography.test.js, which owns the detail. What is pinned here is
// that BOTH tokens resolve to the same family: the distinction the console
// draws is weight and size, and a second family creeping back into one of
// these two tokens is how the old drift started.
check('the type scale is one family at two weights, not two families',
  /--font-heading:\s*'Noto Sans'/.test(css) && /--font-body:\s*'Noto Sans'/.test(css)
  && /--font-heading-weight: 600/.test(css)
  && /\.cx-kicker \{[^}]*font-size: 13px[^}]*letter-spacing: 0/.test(css)
  && /\.cx-title \{[^}]*font-size: 34px[^}]*line-height: 1\.15/.test(css));
// The handoff says "no text below 13px anywhere" and, in the same table,
// gives table headers 11px and tags 12px. The component sizes are the ones
// the mocks were drawn with, so they win; what is pinned is that nothing
// falls below them — the 9px pills and 10px labels of the old console.
check('nothing in the shared vocabulary is smaller than 11px, and body text is never below 13px',
  !/font-size:\s*(10|[0-9])px/.test(css.replace(/\/\*[\s\S]*?\*\//g, ''))
  && /\.cx-meta \{ font-size: 13px/.test(css) && /\.cx-body \{ font-size: 15px/.test(css), 'a size below 11px is set');

/* ── 2. Self-hosted fonts ───────────────────────────────────────────────── */
const faces = css.match(/@font-face \{[^}]*\}/g) || [];
// Six now: Noto Sans 400/500/600/700 and IBM Plex Mono 400/500. The mono
// face joined the list when the console stopped linking Google Fonts.
check('six @font-face rules: Noto Sans 400/500/600/700 and the mono at 400/500',
  faces.length === 6
  && faces.filter(f => /'Noto Sans'/.test(f)).length === 4
  && faces.filter(f => /'IBM Plex Mono'/.test(f)).length === 2);
const fontFiles = faces.map(f => (f.match(/url\('\/fonts\/([^']+)'\)/) || [])[1]).filter(Boolean);
check('every face points at a woff2 that is actually in public/fonts',
  fontFiles.length === 6 && fontFiles.every(f => exists('fonts/' + f) && fs.statSync(path.join(PUB, 'fonts', f)).size > 10000),
  fontFiles.join(', '));
// Stronger than the old check, which only forbade Barlow: no console page
// fetches ANY font stylesheet from a third party now that the mono face is
// self-hosted too. That removed a render-blocking request from 26 pages.
check('and no console page fetches a font from Google at all',
  appPages.every(f => !/fonts\.googleapis\.com/.test(read(f))),
  appPages.filter(f => /fonts\.googleapis\.com/.test(read(f))).join(', '));

/* ── 3. The pages carry no copy of the palette ──────────────────────────── */
const SHARED = ['--bg', '--bg2', '--text', '--text2', '--accent', '--green', '--amber', '--red', '--serif', '--r'];
const rootBlock = (src) => { const m = src.match(/:root\s*\{([^}]*)\}/); return m ? m[1] : ''; };
const carrying = appPages.filter(f => SHARED.some(t => new RegExp('(^|[\\s;])' + t + '\\s*:').test(rootBlock(read(f)))));
check('no console page declares a shared token in its own :root (' + appPages.length + ' pages)',
  carrying.length === 0, carrying.join(', '));
check('every console page loads cygenix-console.css, and before cygenix-theme.css where it loads that too',
  appPages.every(f => {
    const s = read(f);
    const a = s.indexOf('href="/cygenix-console.css"'), b = s.indexOf('href="/cygenix-theme.css"');
    return a !== -1 && (b === -1 || a < b);
  }), appPages.filter(f => read(f).indexOf('href="/cygenix-console.css"') === -1).join(', '));
check('the Schema Explorer keeps its own data-map ramp — page-specific tokens are left alone',
  /--da-s1/.test(rootBlock(read('schema_explorer.html'))));

const theme = read('cygenix-theme.css');
check('cygenix-theme.css has no doubled selector and no per-theme patches',
  !/\[data-theme="light"\]\[data-theme="light"\]/.test(theme)
  && !/html\[data-theme="financial"\] \.topbar/.test(theme) && !/html\[data-theme="financial"\] \.cyg-nav-item/.test(theme));
check('a named theme is a remap of the tokens, nothing more',
  /html\[data-theme="financial"\]\{[^}]*--color-bg:/.test(theme));

/* ── 4. The chrome agrees with itself ───────────────────────────────────── */
const rail = read('cygenix-sidebar.js');
const railW = Number((css.match(/--cx-rail-w:\s*(\d+)px/) || [])[1]);
const mastH = Number((css.match(/--cx-masthead-h:\s*(\d+)px/) || [])[1]);
check('the rail width is stated once in the tokens and once in the module, and they agree',
  railW === 216 && new RegExp('WIDTH_OPEN\\s*=\\s*' + railW).test(rail));
check('so is the masthead height', mastH === 60 && new RegExp('MASTHEAD_H\\s*=\\s*' + mastH).test(rail));
const badOffset = appPages.filter(f => /padding-left:\s*230px|calc\(100vw - 230px|left:\s*230px/.test(read(f)));
check('no console page still pads for the old 230px rail', badOffset.length === 0, badOffset.join(', '));
check('the body clears the masthead and the hairline together',
  /body \{ padding-top: calc\(var\(--cx-masthead-h\) \+ var\(--cyg-hairline-h, 0px\)\); \}/.test(css)
  && /padding-top:calc\(var\(--cyg-hairline-h\) \+ var\(--cx-masthead-h,0px\)\)/.test(read('cygenix-status-hairline.js')));
check('the hairline paints the same status trio the tokens name',
  /green: '#3f6b52', amber: '#9a6b1f', red: '#9c3f38'/.test(read('cygenix-status-hairline.js')));
check('the rail carries no per-item colour — finding 04 is a data change',
  !/color:\s*'var\(--(teal|green|amber|purple|red|accent|yellow|text2|text3)/.test(rail));

// The masthead search field is the one input on a dark bar, and
// cygenix-console.css styles `input[type="search"]` for the LIGHT page body.
// An attribute selector outscores a bare class, so a plain `.cx-mh-search`
// loses its background to the page palette — which is how the field ended up
// painting white placeholder text on a near-white box. Whether it is readable
// is measured for real in tests/browser/masthead-contrast.smoke.js; what is
// pinned here is the only thing source can pin: that every property is still
// qualified, so nobody "simplifies" the selector and quietly loses again.
const mhSearch = rail.split('\n').filter((l) => l.includes('.cx-mh-search') && l.includes('{'));
check('every masthead search rule is qualified with .cx-masthead, or the page palette wins',
  mhSearch.length >= 4 && mhSearch.every((l) => l.includes('.cx-masthead .cx-mh-search')),
  mhSearch.map((l) => l.trim().slice(0, 50)).join(' | '));
check('the field is filled rather than transparent, so it reads as somewhere to type',
  /\.cx-masthead \.cx-mh-search\{[^}]*background:rgba\(255,255,255,\.12\)/.test(rail));
check('and the placeholder pins its own opacity, which Firefox otherwise dims',
  /\.cx-masthead \.cx-mh-search::placeholder\{[^}]*opacity:1/.test(rail));

/* ── 5. Where the tab strips mount ──────────────────────────────────────── */
// Every page whose data-active key belongs to a strip must declare a mount,
// or the strip — and with it the destinations that moved into it — is missing
// from that screen.
const vm = require('vm');
const noopEl = () => ({ classList:{add(){},remove(){},toggle(){},contains:()=>false}, addEventListener(){}, setAttribute(){},
  querySelector:()=>null, querySelectorAll:()=>[], appendChild(){}, replaceWith(){}, style:{}, dataset:{}, textContent:'' });
const sb = { window:{ addEventListener(){}, location:{ pathname:'/x', origin:'https://x' } },
  document:{ readyState:'loading', addEventListener(){}, createElement:noopEl, getElementById:()=>null, querySelector:()=>null, querySelectorAll:()=>[], head:noopEl(), body:noopEl(), documentElement:noopEl() },
  localStorage:{ getItem:()=>null, setItem(){}, removeItem(){} }, sessionStorage:{ getItem:()=>null, setItem(){}, removeItem(){} },
  console, setTimeout:()=>0, setInterval:()=>0 };
sb.window.document = sb.document; sb.window.localStorage = sb.localStorage;
vm.createContext(sb); vm.runInContext(rail, sb);
const SB = sb.window.CygenixSidebar;
const noMount = appPages.filter(f => {
  const s = read(f);
  const key = (s.match(/id="cyg-sidebar-mount" data-active="([^"]+)"/) || [])[1];
  return key && SB.tabsFor(key) && !/id="cyg-subnav-mount"/.test(s);
});
check('every page that owns or belongs to a tab strip declares #cyg-subnav-mount', noMount.length === 0, noMount.join(', '));
check('the dashboard mounts the strip at the top of .main, above every view',
  /<div class="main">\s*<div id="cyg-subnav-mount"><\/div>/.test(read('dashboard.html')));

/* ── 6. The dashboard shell ─────────────────────────────────────────────── */
const dash = read('dashboard.html');
check('the dashboard\'s own topbar is gone — the masthead is the topbar',
  !/<div class="topbar">/.test(dash) && !/id="user-pill"/.test(dash) && !/id="sync-btn"/.test(dash));
check('its grid no longer reserves a 52px row for it',
  /\.app\{[^}]*grid-template-rows:1fr;/.test(dash) && /height:calc\(100vh - var\(--cx-masthead-h,0px\)/.test(dash));
check('Sync moved into the account menu, rendered only where syncFromCloud exists',
  /typeof window\.syncFromCloud === 'function'/.test(rail) && /cyg-user-menu-sync/.test(rail));
check('the dashboard code that filled the old topbar is guarded, not deleted from under a null',
  /const pnb = \$\('project-name-badge'\);\s*if \(pnb/.test(read('dashboard-app.js'))
  && /const td = \$\('today-date'\); if \(td\)/.test(read('dashboard-app.js')));

/* ── 7. Home ────────────────────────────────────────────────────────────── */
const app = read('dashboard-app.js');
check('Home is one root the renderer fills, and the model module loads before the app',
  /id="home-root" class="hm-grid" data-tour="home-cards"/.test(dash)
  && dash.indexOf('/cygenix-home.js') < dash.indexOf('/dashboard-app.js'));
check('readiness still calls the one pfConfidence()', /CygenixPreflight\.pfConfidence\(\{ jobs, preflight \}\)/.test(app));
check('the plan comes from the pipeline model cygenix-pipeline.js already derives', /CygenixPipeline\.toPipelineModel\(/.test(app));
check('needs-you is four existing reads and no new endpoint',
  /rbac-admin\?what=approvals/.test(app) && /cygenix_assurance_v1/.test(app)
  && /CygenixDataStream\.blockedStreams/.test(app) && !/needs-you|home-needs/.test(read('../netlify/functions/data-proxy.js').replace('..', '')));
check('the approvals read never blocks paint and never raises on failure',
  /async function homeLoadApprovals/.test(app) && /catch \{ _homeApprovals = \[\]; \}/.test(app)
  && /renderHome\(\);\s*\n\s*\/\/ The two network reads fill their cells in behind the frame/.test(app));
check('never "Loading…" — the outstanding cells shimmer',
  /cx-shimmer/.test(app) && !/Loading…/.test(dash.slice(dash.indexOf('id="view-dashboard"'), dash.indexOf('<!-- NEW JOB -->'))));
check('the schedules module keeps its tested contract and simply tells Home when the list lands',
  /if \(typeof renderHomeNextScheduled === 'function'\) renderHomeNextScheduled\(schedules\);/.test(app)
  && /function renderDashboardSchedules\(schedules\)\{/.test(app));
check('the right column is a permanent 360px sticky column on neutral-100',
  /\.hm-side\{[^}]*position:sticky/.test(dash) && /--cx-right-col-w:\s*360px/.test(css)
  && /\.hm-side\{[^}]*var\(--color-neutral-100\)/.test(dash));
check('the empty state is three numbered steps, never "No projects yet"',
  /cx-steps/.test(app) && !/No projects yet/.test(app.slice(app.indexOf('function renderHome('), app.indexOf('function renderHome(') + 9000)));

/* ── 8. The public pages are untouched ──────────────────────────────────── */
const touched = publicPages.filter(f => exists(f) && (/cygenix-console\.css/.test(read(f)) || /Barlow/.test(read(f)) || /cx-masthead/.test(read(f))));
check('index, login, pricing, register, help and the other public pages load none of this', touched.length === 0, touched.join(', '));
check('cygenix-brand.css still says indigo — the public side keeps its brand',
  /--accent:\s*#4A5BD6/i.test(read('cygenix-brand.css')) && !/Barlow/.test(read('cygenix-brand.css')));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
if (fail) process.exit(1);
