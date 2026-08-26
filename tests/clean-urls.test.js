// tests/clean-urls.test.js — the site's addresses.
//
// Two rules, and this file is what stops either of them rotting:
//
//   1. The landing page is the root, and the root is its only address —
//      /index.html and /index both redirect there rather than serving a
//      second copy of the same page at a second URL.
//   2. No .html in any address a person can see. Every page answers at its
//      extensionless address, and the old .html address permanently redirects
//      to it so bookmarks and indexed links heal rather than serving a second
//      copy of the same page at a second URL.
//   3. Addresses are hyphenated and named for the screen, not the file:
//      /user-roles, not /user_roles; /configurator, not /effort_estimator.
//      Every address a page has ever answered at still redirects to the
//      current one.
//
// The routing table is generated from the pages on disk (scripts/build-routes.js)
// rather than hand-kept, so this file re-runs the generator and fails if what
// is committed has drifted — which is what would happen the first time somebody
// adds a page and forgets.

'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));

const ROOT = path.join(__dirname, '..');
const PUB = path.join(ROOT, 'public');
const routes = require(path.join(ROOT, 'scripts', 'build-routes.js'));

const redirects = fs.readFileSync(path.join(PUB, '_redirects'), 'utf8');
const pages = routes.pages();
const stems = new Set(pages.map(f => f.replace(/\.html$/, '')));

// One rule per line: from, to, status.
const rules = redirects.split('\n')
  .map(l => l.trim())
  .filter(l => l && !l.startsWith('#'))
  .map(l => { const [from, to, status] = l.split(/\s+/); return { from, to, status }; });

const ruleFor = (from) => rules.find(r => r.from === from);

console.log('Clean URLs — what a visitor sees in the address bar\n');

// ── 1. The generated table is current ───────────────────────────────────────
section('1. The routing table is generated, not remembered');
check('the committed _redirects is exactly what the generator produces',
  redirects === routes.build(),
  're-run: node scripts/build-routes.js');
check('the build regenerates it, so a deploy cannot ship a stale table',
  /build-routes\.js/.test(fs.readFileSync(path.join(ROOT, 'netlify.toml'), 'utf8')));
check('page routing lives in one file — netlify.toml declares only API proxies',
  !/from = "\/[a-z0-9_-]+(\.html)?"/i.test(
    fs.readFileSync(path.join(ROOT, 'netlify.toml'), 'utf8').replace(/from = "\/api\/[^"]*"/g, '')),
  'a page redirect in netlify.toml would compete with _redirects');

// ── 2. The root ─────────────────────────────────────────────────────────────
section('2. The landing page is the root, and the root is its only address');
check('the root is left alone — the server serves index.html there by default',
  !ruleFor('/'),
  'a rule on / would mean something other than the landing page answers it');
check('the file name redirects to the root rather than serving a second copy',
  (ruleFor('/index.html') || {}).to === '/' && (ruleFor('/index.html') || {}).status === '301!');
check('and so does the bare /index',
  (ruleFor('/index') || {}).to === '/' && (ruleFor('/index') || {}).status === '301!');
// /home existed for exactly one deploy, while the root belonged to the login
// screen. Anything that saw it then should land somewhere, not on a 404.
check('/home, which the landing page briefly answered to, redirects to the root',
  (ruleFor('/home') || {}).to === '/' && (ruleFor('/home') || {}).status === '301!');

// ── 3. Every page, both ways ────────────────────────────────────────────────
section('3. Every page answers clean, and its .html address heals');
const missingClean = [], missingHeal = [];
for (const f of pages) {
  if (f === routes.LANDING) continue;
  const clean = routes.addressOf(f);
  const serve = ruleFor(clean);
  if (!serve || serve.to !== '/' + f || serve.status !== '200') missingClean.push(clean);
  if (f === routes.CALLBACK_PAGE) continue;            // see below
  const heal = ruleFor('/' + f);
  if (!heal || heal.to !== clean || heal.status !== '301!') missingHeal.push('/' + f);
}
check('every page is served at its extensionless address (' + (pages.length - 1) + ')',
  missingClean.length === 0, missingClean.slice(0, 5).join(', '));
check('every .html address permanently redirects to it',
  missingHeal.length === 0, missingHeal.slice(0, 5).join(', '));
// ── 3b. Addresses read like a product, not like a directory listing ────────
section('3b. Addresses are hyphenated, and named for the screen');
const underscored = pages.map(f => routes.addressOf(f)).filter(a => a.includes('_'));
check('no address contains an underscore', underscored.length === 0, underscored.join(', '));
check('the Configurator is at /configurator, not the name it used to have',
  routes.addressOf('effort_estimator.html') === '/configurator');
// A renamed address is only safe if the one it replaced still goes somewhere.
const unhealed = [];
for (const f of pages) {
  const addr = routes.addressOf(f), was = routes.defaultAddressOf(f);
  if (addr === was) continue;
  const r = ruleFor(was);
  if (!r || r.to !== addr || r.status !== '301!') unhealed.push(was);
}
check('every address a page used to answer at redirects to its current one',
  unhealed.length === 0, unhealed.join(', '));
check('and /project-plan belongs to the Project Plan screen rather than bouncing to the dashboard',
  (ruleFor('/project-plan') || {}).to === '/project_plan.html',
  JSON.stringify(ruleFor('/project-plan')));

check('the redirect is forced, so an existing file does not win over the rule',
  rules.filter(r => r.from.endsWith('.html') && r.from !== '/' + routes.CALLBACK_PAGE)
       .every(r => r.status === '301!'),
  'without ! Netlify serves the file and the old URL stays alive');

// The one exemption, and the reason it exists. Entra returns the code in the
// URL fragment; a 301 on the callback address puts MSAL on a URL that no
// longer matches the redirect_uri it asked for, and sign-in fails. This is a
// deliberate hole and the test names it so nobody closes it by tidying.
check('the sign-in callback address is SERVED, not redirected',
  (ruleFor('/' + routes.CALLBACK_PAGE) || {}).status === '200'
  && (ruleFor('/' + routes.CALLBACK_PAGE) || {}).to === '/' + routes.CALLBACK_PAGE,
  'a 301 here breaks sign-in — the auth fragment does not survive it');

// ── 4. Nothing in the product still points at a .html address ───────────────
section('4. No link, script or redirect hands a visitor a .html address');
const offenders = [];
const callbackRefs = [];
const REF = /["'`]((?:\/)?[a-z0-9_-]+)\.html(?=[#?"'`])/g;
// An OAuth callback is not a link. It is a value handed to Entra, which will
// only accept one it has registered, so it is exempt from the clean-address
// rule — and collected separately rather than ignored, so the count below can
// assert it is the ONLY thing exempt.
const OAUTH = /redirectUri:|postLogoutRedirectUri:|post_logout_redirect_uri=/;
for (const f of fs.readdirSync(PUB)) {
  if (!/\.(html|js)$/.test(f)) continue;
  const src = fs.readFileSync(path.join(PUB, f), 'utf8');
  for (const line of src.split('\n')) {
    let m;
    REF.lastIndex = 0;
    while ((m = REF.exec(line))) {
      const stem = m[1].replace(/^\//, '');
      if (!stems.has(stem)) continue;
      if (OAUTH.test(line)) callbackRefs.push(f + ' → ' + m[0].slice(1));
      else offenders.push(f + ' → ' + m[0].slice(1));
    }
  }
}
check('no page or script LINKS to a .html address', offenders.length === 0,
  offenders.slice(0, 6).join(' | '));
check('the only .html left in the product is the OAuth callback',
  callbackRefs.length > 0 && callbackRefs.every(r => r.endsWith('/' + routes.CALLBACK_PAGE)),
  callbackRefs.filter(r => !r.endsWith('/' + routes.CALLBACK_PAGE)).join(' | '));

// A reference in prose is just as visible as one in an href.
const prose = [];
for (const f of fs.readdirSync(PUB)) {
  if (!f.endsWith('.html')) continue;
  const text = fs.readFileSync(path.join(PUB, f), 'utf8')
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ');
  for (const m of text.matchAll(/\/([a-z0-9_-]+)\.html/g)) {
    if (stems.has(m[1])) prose.push(f + ' → ' + m[0]);
  }
}
check('nor names one in visible copy', prose.length === 0, prose.slice(0, 5).join(' | '));

// ── 5. The gate understands the new shape ───────────────────────────────────
section('5. The auth gate reads extensionless paths');
const gate = fs.readFileSync(path.join(PUB, 'auth-gate.js'), 'utf8');
check('it strips a trailing .html before classifying a path',
  /replace\(\/\\\.html\$\/, ''\)/.test(gate));
check('its public list is extensionless',
  !/PUBLIC = \[[^\]]*\.html/s.test(gate),
  (gate.match(/PUBLIC = \[[^\]]*\]/s) || [''])[0].slice(0, 120));
check('it matches exactly rather than by suffix',
  /indexOf\(path\) !== -1/.test(gate),
  'endsWith would let /anything/login through the gate');
check('the root is public, because the root is the landing page',
  /PUBLIC = \[\s*'\/'/.test(gate));

// ── 6. The logos ────────────────────────────────────────────────────────────
section('6. The logos point somewhere useful');
const index = fs.readFileSync(path.join(PUB, 'index.html'), 'utf8');
const login = fs.readFileSync(path.join(PUB, 'login.html'), 'utf8');
check('the landing page\'s logo goes to the root, which is the landing page',
  /<a class="nav-logo" href="\/" aria-label="Cygenix home">/.test(index));
check('its canonical address is the bare domain',
  /og:url" content="https:\/\/cygenix\.co\.uk\/"/.test(index));
check('the login page offers a way back out to the site',
  /class="brand-logo" href="\/"/.test(login));
check('no page still points at /home, which no longer serves anything',
  !fs.readdirSync(PUB).filter(f => /\.(html|js)$/.test(f))
    .some(f => /["'`]\/home["'`]/.test(fs.readFileSync(path.join(PUB, f), 'utf8'))));

// ── 7. The sign-in callback ─────────────────────────────────────────────────
// MSAL hands Entra a redirect_uri and Entra refuses anything not registered
// against the app, so this value is not free to drift. One address, in one
// shape, everywhere — a second one would mean a second Entra entry nobody
// remembers to add, and the failure is a total sign-in outage.
section('7. The sign-in callback is one registered address');
const CALLBACK = /window\.location\.origin \+ '([^']+)'/g;
const callbacks = new Set();
for (const f of fs.readdirSync(PUB)) {
  if (!/\.(html|js)$/.test(f)) continue;
  const src = fs.readFileSync(path.join(PUB, f), 'utf8');
  const auth = src.split('\n')
    .filter(l => /redirectUri|post_?[Ll]ogout/.test(l))
    .join('\n');
  let m;
  while ((m = CALLBACK.exec(auth))) callbacks.add(m[1]);
}
check('every sign-in and sign-out callback names the same address',
  callbacks.size === 1, [...callbacks].join(', ') || 'none found');
check('and it is the address registered in Entra, not the clean one',
  callbacks.has('/' + routes.CALLBACK_PAGE), [...callbacks].join(', '));
check('the deploy note explains why the callback keeps its extension',
  /AADSTS50011/.test(fs.readFileSync(path.join(ROOT, 'docs', 'DEPLOY-clean-urls.md'), 'utf8')),
  'sign-in breaks if this is changed without registering the new URI first');

// Ordinary navigation to the sign-in screen is a link, not a registered URI,
// and stays clean. Mixing the two up is what broke sign-in the first time.
const gateSrc = fs.readFileSync(path.join(PUB, 'auth-gate.js'), 'utf8');
check('but ordinary navigation to sign-in still uses the clean address',
  /location\.replace\('\/login\?reason=protected'\)/.test(gateSrc),
  'only the OAuth callback needs the registered form');

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
