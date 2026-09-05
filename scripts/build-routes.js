/* scripts/build-routes.js — generate public/_redirects from the pages on disk.
 * ---------------------------------------------------------------------------
 * Two rules the site now keeps, and this file is what enforces them:
 *
 *   1. The root is the landing page, as it always was. index.html is served
 *      at / and has no second address: /index.html and /index both redirect
 *      there, so the page a visitor lands on has exactly one URL.
 *
 *   2. No .html in any address. /dashboard serves dashboard.html, and
 *      /dashboard.html permanently redirects to /dashboard so old links,
 *      bookmarks and anything already indexed heal themselves rather than
 *      quietly serving a second copy of the page at a second URL.
 *
 *   3. Addresses are hyphenated and named for the screen, not the file.
 *      /user-roles, not /user_roles; /configurator, not /effort_estimator.
 *      A file keeps its name on disk — renaming 48 files to fix an address
 *      would be a large diff to change something nobody sees — and ADDRESS
 *      below maps the two where they differ.
 *
 * The list is generated rather than hand-kept, because a hand-kept list is one
 * a new page gets forgotten from — and the failure is silent, a 404 nobody
 * notices until a customer finds it. tests/clean-urls.test.js re-runs this
 * generator and fails the build if the committed file has drifted.
 *
 * Netlify matches _redirects top to bottom, first match wins, and the file
 * takes precedence over netlify.toml. Order below is deliberate:
 * the specific roots first, then the .html cleanups, then the rewrites.
 */

'use strict';

const fs = require('fs');
const path = require('path');

const PUBLIC = path.join(__dirname, '..', 'public');
const OUT = path.join(PUBLIC, '_redirects');

// The landing page is the one file whose address is not its name: it is
// index.html on disk and simply / in an address.
const LANDING = 'index.html';
const LANDING_URL = '/';

// The one page whose .html address must stay alive.
//
// MSAL hands Entra a redirect_uri and Entra will only send the browser back to
// a URI registered against the app. That registration is
// https://cygenix.co.uk/login.html, so the callback lands there — and it must
// land, not be redirected. Entra returns the authorization code in the URL
// FRAGMENT (response_mode=fragment); bouncing that through a 301 puts MSAL on
// a URL that no longer matches the redirect_uri it asked for, and it refuses
// to complete the sign-in.
//
// /login still works as an ordinary address and is what every link uses. This
// exemption exists only so the callback has somewhere to land.
//
// To retire it: register https://cygenix.co.uk/login in Entra (App
// registrations - Authentication - Redirect URIs), point the callbacks at
// '/login' again, and delete this constant.
const CALLBACK_PAGE = 'login.html';

// Where a page's address is not simply its file name.
//
// Two reasons appear here. Underscores: file names have carried them since the
// beginning and an address should not, because /data_stream_store reads like a
// path on somebody's disk and /data-stream-store reads like a product.
// Renames: the Effort Estimator became the Configurator, and the address
// should say what the screen is called rather than what it used to be.
//
// Every address a page has ever answered to keeps working — the generator
// emits a 301 from the file-name form as well as from the previous address.
const ADDRESS = {
  'agentive_migration.html':    '/agentive-migration',
  'data_stream.html':           '/data-stream',
  'data_stream_designer.html':  '/data-stream-designer',
  'data_stream_events.html':    '/data-stream-events',
  'data_stream_monitor.html':   '/data-stream-monitor',
  'data_stream_store.html':     '/data-stream-store',
  'effort_estimator.html':      '/configurator',
  'object_mapping.html':        '/object-mapping',
  'project_plan.html':          '/project-plan',
  'report_settings.html':       '/report-settings',
  'schema_explorer.html':       '/schema-explorer',
  'user_roles.html':            '/user-roles',
};

// The address a page answers at, and the one its file name would have given
// it. They differ only for the entries above.
function addressOf(file) {
  return ADDRESS[file] || '/' + file.replace(/\.html$/, '');
}
function defaultAddressOf(file) {
  return '/' + file.replace(/\.html$/, '');
}

// Addresses that no longer have a page behind them. A bookmark or an old link
// should land somewhere useful rather than on a 404. These live here rather
// than in netlify.toml so there is one file that decides where a URL goes.
const LEGACY = [
  // /project-plan used to send people to the dashboard, because the page that
  // answered it had been removed. That address now belongs to the live Project
  // Plan screen — which is where somebody following an old Project Planner
  // link wanted to go in the first place — so the rule is gone rather than
  // competing with it.
  //
  // /home was the landing page's address for one deploy, while the root
  // briefly belonged to the sign-in screen. Anything that saw it in that
  // window is sent to the root rather than to a 404.
  ['/home',              '/'],
  // The Data Analyser was its own page for a short while. It now lives on
  // the Data import tab of Connections, and the dashboard's goto reader
  // takes a view and a tab (see the deep-link block in dashboard-app.js).
  // A query string rather than a fragment, because a fragment does not
  // reliably survive a server-side redirect and a query string does. Both
  // the clean address and the file name are covered: with the page gone,
  // the generated .html→clean rule below no longer exists for it.
  ['/data-analyser',      '/dashboard?goto=connections/import'],
  ['/data-analyser.html', '/dashboard?goto=connections/import'],
];

function pages() {
  return fs.readdirSync(PUBLIC)
    .filter(f => f.endsWith('.html'))
    .sort();
}

function build() {
  const list = pages();
  const L = [];

  L.push('# ─────────────────────────────────────────────────────────────────');
  L.push('# GENERATED by scripts/build-routes.js — do not edit by hand.');
  L.push('# Re-run: node scripts/build-routes.js');
  L.push('# tests/clean-urls.test.js fails the build if this drifts.');
  L.push('# ─────────────────────────────────────────────────────────────────');
  L.push('');
  L.push('# The root is the landing page, and it is the only address for it:');
  L.push('# the file name and the bare /index both redirect here.');
  L.push(pad('/' + LANDING, 22) + pad(LANDING_URL, 22) + '301!');
  L.push(pad('/index', 22) + pad(LANDING_URL, 22) + '301!');
  L.push('# Addresses whose page is gone.');
  for (const [from, to] of LEGACY) L.push(pad(from, 22) + pad(to, 22) + '301!');
  L.push('');
  L.push('# The sign-in callback. Entra sends the browser back to this exact');
  L.push('# address with the code in the fragment, so it is served rather than');
  L.push('# redirected — a 301 here breaks sign-in. See scripts/build-routes.js.');
  L.push(pad('/' + CALLBACK_PAGE, 22) + pad('/' + CALLBACK_PAGE, 22) + '200');
  L.push('');
  L.push('# Old .html addresses heal themselves. 301! forces the redirect even');
  L.push('# though the file exists at that path, which is the whole point.');
  for (const f of list) {
    if (f === LANDING) continue;                       // handled above
    if (f === CALLBACK_PAGE) continue;                 // must not be redirected
    L.push(pad('/' + f, 26) + pad(addressOf(f), 24) + '301!');
  }
  L.push('');
  L.push('# Pages whose address is not their file name send the file-name form');
  L.push('# here too, so an address a page used to answer at still works.');
  for (const f of list) {
    const addr = addressOf(f), fallback = defaultAddressOf(f);
    if (addr === fallback) continue;
    L.push(pad(fallback, 26) + pad(addr, 24) + '301!');
  }
  L.push('');
  L.push('# …and the address serves the file.');
  for (const f of list) {
    if (f === LANDING) continue;
    L.push(pad(addressOf(f), 26) + pad('/' + f, 24) + '200');
  }
  L.push('');
  return L.join('\n');
}

function pad(s, n) { return s + ' '.repeat(Math.max(1, n - s.length)); }

const next = build();
const prev = fs.existsSync(OUT) ? fs.readFileSync(OUT, 'utf8') : '';

if (require.main === module) {
  if (prev === next) {
    console.log('build-routes: already current — ' + pages().length + ' page(s)');
  } else {
    fs.writeFileSync(OUT, next);
    console.log('build-routes: written — ' + pages().length + ' page(s)');
  }
}

module.exports = { build, pages, LANDING, LANDING_URL, CALLBACK_PAGE, ADDRESS,
                   addressOf, defaultAddressOf, OUT };
