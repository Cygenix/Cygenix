// tests/ga4-analytics.test.js — Google Analytics on the public site, and
// the promises made around it.
//
// WHY THIS FILE EXISTS
// Three things here are the kind that go wrong quietly.
//
//   1. WHICH PAGES CARRY THE TAG. Analytics belongs on the seven public
//      marketing pages and on none of the forty-two behind the sign-in.
//      Nothing about a page's markup announces which sort it is, so a new
//      page copied from an old one can pick up the tag by accident and
//      start measuring customers inside their own migration console. The
//      classification is therefore written down HERE, as data, and the
//      file listing is checked against it: add a page without classifying
//      it and this test fails with its name.
//
//   2. WHETHER GOOGLE LOADS BEFORE CONSENT. The claim is that nothing is
//      fetched from Google until somebody presses Accept. That is not
//      provable by reading the source, so the module is EXECUTED here
//      against a fake window, and what it appends to the document head is
//      counted.
//
//   3. WHETHER THE PRIVACY POLICY IS STILL TRUE. It used to say the site
//      used no analytics. Shipping analytics made that sentence false, and
//      a false privacy policy is a compliance problem rather than a typo.
//
// Plain Node, no framework, no browser.

'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));
const read = (...p) => fs.readFileSync(path.join(__dirname, '..', ...p), 'utf8');

const PUBLIC = path.join(__dirname, '..', 'public');
const GA = read('public', 'cygenix-ga4.js');
const CC = read('public', 'cookie-consent.js');
const PRIVACY = read('public', 'privacy.html');
const TOML = read('netlify.toml');

const MEASUREMENT_ID = 'G-K3NJP5GX2G';

// ── The classification, as data ───────────────────────────────────────────
// Public marketing: reachable signed out, brand styling, linked from the
// marketing navigation (about is an orphan but is marketing copy and
// serves at /about). Everything else is the product.
const MARKETING = [
  'about.html', 'help.html', 'index.html', 'pricing.html',
  'privacy.html', 'register.html', 'terms.html',
];

(async () => {
  console.log('Analytics on the public site — who is measured, and when\n');

  /* ── 1. Which pages carry the tag ──────────────────────────────────── */
  section('1. The tag is on the marketing pages and nowhere else');

  const pages = fs.readdirSync(PUBLIC).filter(f => f.endsWith('.html')).sort();
  const app = pages.filter(p => MARKETING.indexOf(p) === -1);

  const unclassified = MARKETING.filter(p => pages.indexOf(p) === -1);
  check('every page named as marketing still exists', unclassified.length === 0, unclassified.join(', '));
  check('the site still has the pages this file was written against (49)',
    pages.length === 49, 'found ' + pages.length + ' — classify the new one in MARKETING or leave it out deliberately');

  const carries = (p) => /cygenix-ga4\.js/.test(fs.readFileSync(path.join(PUBLIC, p), 'utf8'));
  const missing = MARKETING.filter(p => !carries(p));
  check('ALL SEVEN MARKETING PAGES LOAD THE ANALYTICS MODULE', missing.length === 0, missing.join(', '));

  const leaked = app.filter(carries);
  check('NO PAGE BEHIND THE SIGN-IN LOADS IT', leaked.length === 0, leaked.join(', '));

  // The real failure mode is a raw gtag snippet pasted into a page, which
  // would bypass the consent module entirely.
  const rawGtag = pages.filter((p) => {
    const s = fs.readFileSync(path.join(PUBLIC, p), 'utf8');
    return /googletagmanager\.com|gtag\(/.test(s);
  });
  check('no page carries a hand-pasted Google tag — the module is the only route',
    rawGtag.length === 0, rawGtag.join(', '));

  check('dashboard, login and admin are explicitly among the pages without it',
    ['dashboard.html', 'login.html', 'admin.html'].every(p => app.indexOf(p) !== -1 && !carries(p)));

  const headFirst = MARKETING.every((p) => {
    const s = fs.readFileSync(path.join(PUBLIC, p), 'utf8');
    const h = s.indexOf('<head>');
    const g = s.indexOf('cygenix-ga4.js');
    const other = s.indexOf('<script', h + 6);
    return h > -1 && g > h && (other === -1 || g < s.indexOf('</script>', other) + 4000);
  });
  check('the tag sits at the top of the head, so the denied default is set before anything else runs', headFirst);

  check('every marketing page also carries the consent banner — a tag without a banner is the compliance failure',
    MARKETING.every(p => /cookie-consent\.js/.test(fs.readFileSync(path.join(PUBLIC, p), 'utf8'))));

  /* ── 2. The module, executed ───────────────────────────────────────── */
  section('2. Run it: nothing reaches Google before Accept');

  // A browser in the small. Records every script appended to the head and
  // every cookie written.
  function browser(stored) {
    const appended = [];
    const cookies = [];
    const listeners = {};
    const store = {};
    if (stored !== undefined) store['cygenix_cookie_consent'] = stored;
    const win = {
      localStorage: {
        getItem: (k) => (k in store ? store[k] : null),
        setItem: (k, v) => { store[k] = String(v); },
        removeItem: (k) => { delete store[k]; },
      },
      document: {
        head: { appendChild: (el) => appended.push(el) },
        createElement: () => ({ set src(v) { this._src = v; }, get src() { return this._src; } }),
        set cookie(v) { cookies.push(v); },
        get cookie() { return ''; },
      },
      location: { hostname: 'www.cygenix.co.uk' },
      addEventListener: (t, fn) => { (listeners[t] = listeners[t] || []).push(fn); },
      navigator: { userAgent: 'test' },
      Date, JSON, Array, Object, String, Number, RegExp, Math, Error, console,
    };
    win.window = win;
    win.CustomEvent = class { constructor(t, i) { this.type = t; this.detail = i && i.detail; } };
    vm.runInNewContext(GA, win);
    return {
      win, appended, cookies, store,
      fire: (detail) => (listeners['cygenix:cookie-consent'] || []).forEach(fn => fn({ detail })),
      // The dataLayer holds the arguments objects gtag pushed.
      calls: () => win.dataLayer.map(a => Array.prototype.slice.call(a)),
    };
  }

  // No stored consent at all — a first-time visitor.
  let b = browser(undefined);
  check('A FIRST VISIT LOADS NOTHING FROM GOOGLE', b.appended.length === 0, JSON.stringify(b.appended.map(x => x.src)));
  check('and the module says so', b.win.CygenixGA4.isLoaded() === false);
  const dflt = b.calls().find(c => c[0] === 'consent' && c[1] === 'default');
  check('but a denied default IS set, synchronously, for all four v2 signals',
    dflt && dflt[2].analytics_storage === 'denied' && dflt[2].ad_storage === 'denied'
    && dflt[2].ad_user_data === 'denied' && dflt[2].ad_personalization === 'denied', JSON.stringify(dflt && dflt[2]));
  check('the default is the FIRST thing pushed, before anything could grant',
    b.calls()[0][0] === 'consent' && b.calls()[0][1] === 'default');

  // A stored rejection.
  b = browser(JSON.stringify({ version: '2', essential: true, functional: true, analytics: false }));
  check('a stored REJECTION loads nothing either', b.appended.length === 0 && !b.win.CygenixGA4.isLoaded());
  check('and the stored functional=true does not leak into analytics', b.win.CygenixGA4.consented() === false);

  // A version-1 record, which predates the analytics question.
  b = browser(JSON.stringify({ version: '1', essential: true, functional: true }));
  check('AN OLD (v1) CONSENT RECORD IS NOT TREATED AS A YES — it never mentioned analytics',
    b.appended.length === 0 && b.win.CygenixGA4.consented() === false);

  // Corrupt and hostile stored values.
  b = browser('{not json');
  check('a corrupt record loads nothing and does not throw', b.appended.length === 0);
  b = browser(JSON.stringify({ version: '2', analytics: 'yes' }));
  check('analytics must be the boolean true, not a truthy string', b.appended.length === 0);

  // Storage that throws, as in a private window.
  {
    const win = { document: { head: { appendChild() {} }, createElement: () => ({}), set cookie(v) {}, get cookie() { return ''; } },
      location: { hostname: 'x.test' }, addEventListener() {}, Date, JSON, Array, Object, String, Number, RegExp, Math, Error, console };
    win.window = win;
    win.CustomEvent = class {};
    Object.defineProperty(win, 'localStorage', { get() { throw new Error('blocked'); } });
    let threw = false;
    try { vm.runInNewContext(GA, win); } catch (e) { threw = true; }
    check('storage that throws (a private window) does not throw out of the module', !threw);
  }

  // A stored acceptance.
  b = browser(JSON.stringify({ version: '2', essential: true, functional: false, analytics: true }));
  check('A STORED ACCEPTANCE LOADS GOOGLE, once', b.appended.length === 1 && b.win.CygenixGA4.isLoaded());
  check('from the right URL, with the right measurement id',
    b.appended[0].src === 'https://www.googletagmanager.com/gtag/js?id=' + MEASUREMENT_ID, b.appended[0].src);
  check('the script is async', b.appended[0].async === true);
  let upd = b.calls().filter(c => c[0] === 'consent' && c[1] === 'update');
  check('analytics_storage is updated to granted', upd.length === 1 && upd[0][2].analytics_storage === 'granted');
  check('AND NOTHING EVER GRANTS AN ADVERTISING SIGNAL',
    !b.calls().some(c => c[0] === 'consent' && JSON.stringify(c[2] || {}).indexOf('"ad_') !== -1 && JSON.stringify(c[2]).indexOf('granted') !== -1));
  const cfg = b.calls().find(c => c[0] === 'config');
  check('config is called with the measurement id', cfg && cfg[1] === MEASUREMENT_ID);

  /* ── 3. The event path ─────────────────────────────────────────────── */
  section('3. Pressing Accept, and changing your mind');

  b = browser(undefined);
  check('starts clean', b.appended.length === 0);
  b.fire({ version: '2', analytics: true });
  check('ACCEPT LOADS GOOGLE', b.appended.length === 1 && b.win.CygenixGA4.isLoaded());
  b.fire({ version: '2', analytics: true });
  b.fire({ version: '2', analytics: true });
  check('ACCEPTING AGAIN DOES NOT LOAD IT TWICE — the guard is set before the injection, not by its callback',
    b.appended.length === 1, b.appended.length);

  b.fire({ version: '2', analytics: false });
  upd = b.calls().filter(c => c[0] === 'consent' && c[1] === 'update');
  check('a later rejection updates the signal back to denied',
    upd[upd.length - 1][2].analytics_storage === 'denied');
  check('and deletes the GA cookies, on every host suffix the page could have set them on',
    b.cookies.some(c => /^_ga=/.test(c)) && b.cookies.some(c => c.indexOf('_ga_K3NJP5GX2G=') === 0)
    && b.cookies.some(c => /domain=\.cygenix\.co\.uk/.test(c)) && b.cookies.every(c => /expires=Thu, 01 Jan 1970/.test(c)),
    JSON.stringify(b.cookies.slice(0, 4)));
  const before = b.cookies.length;
  b.fire({ version: '2', analytics: false });
  check('rejecting twice does not redo the deletion', b.cookies.length === before);

  b.fire({ version: '2', analytics: true });
  check('and it can be turned back on in the same page view without a second script',
    b.appended.length === 1 && b.calls().filter(c => c[0] === 'consent' && c[1] === 'update').pop()[2].analytics_storage === 'granted');

  /* ── 4. The source promises ────────────────────────────────────────── */
  section('4. What the source must keep saying');
  check('the module never writes the consent record — one owner for the decision',
    !/setItem\s*\(/.test(GA));
  check('every storage read is wrapped', (GA.match(/try\s*\{/g) || []).length >= 3);
  check('the measurement id appears once, as a constant', (GA.match(/G-K3NJP5GX2G/g) || []).length === 1);
  check('no page-view or user id is sent — config carries no parameters object',
    /gtag\('config', MEASUREMENT_ID\);/.test(GA));

  /* ── 5. The banner ─────────────────────────────────────────────────── */
  section('5. The consent banner');
  check('the record is at version 2, so the analytics question is actually asked',
    /const CONSENT_VERSION = '2'/.test(CC));
  check('an older record is treated as no answer', /c\.version === CONSENT_VERSION \? c : null/.test(CC));
  check('the record carries an analytics field', /analytics,/.test(CC) && /function setConsent\(essential, functional, analytics\)/.test(CC));
  check('Accept grants analytics and Reject does not',
    /function acceptAll\(\)\s*\{\s*setConsent\(true, true, true\)/.test(CC)
    && /function acceptEssential\(\)\s*\{\s*setConsent\(true, false, false\)/.test(CC));
  check('rejectAll and acceptEssential are the same function — the two paths cannot drift',
    /const rejectAll = acceptEssential;/.test(CC));
  check('REJECT AND ACCEPT SHARE ONE STYLE STRING, so they cannot differ in prominence',
    (CC.match(/style="\$\{CHOICE_BTN\}"/g) || []).length === 2
    && /CygenixCookies\.rejectAll\(\)" style="\$\{CHOICE_BTN\}"/.test(CC)
    && /CygenixCookies\.acceptAll\(\)" style="\$\{CHOICE_BTN\}"/.test(CC));
  check('every write announces itself, so the tag can react without polling',
    /dispatchEvent\(new CustomEvent\('cygenix:cookie-consent'/.test(CC));
  check('the storage write is wrapped — a private window can still dismiss the banner',
    /try \{\s*localStorage\.setItem\(CONSENT_KEY/.test(CC));
  check('the panel offers the analytics toggle', /id="cc-analytics"/.test(CC) || /'cc-analytics'/.test(CC));
  check('the old public API still exists — pages call it from inline handlers',
    ['acceptAll', 'acceptEssential', 'openPanel', 'closePanel', 'savePanel', 'getConsent']
      .every(k => new RegExp('window\\.CygenixCookies = \\{[^}]*\\b' + k + '\\b').test(CC)));
  check('the settings button keeps its id, which the nav link and the cleanup script both target',
    /btn\.id = 'cc-settings-btn'/.test(CC));
  check('colour tokens carry literal fallbacks, for the pages that define none',
    /var\(--bg2, #FFFFFF\)/.test(CC) && /var\(--text, #1A1D21\)/.test(CC));

  /* ── 6. The privacy policy ─────────────────────────────────────────── */
  section('6. The privacy policy says what we now do');
  check('THE OLD CLAIM THAT WE USE NO ANALYTICS IS GONE',
    !/analytics services that identify individual users/.test(PRIVACY)
    && !/do not use advertising cookies, tracking pixels, or analytics/.test(PRIVACY));
  check('it names Google Analytics', /Google Analytics/.test(PRIVACY));
  check('it says the measurement happens only with consent',
    /only.{0,40}with your consent|runs <em>only<\/em> with your consent/i.test(PRIVACY));
  check('it says nothing is loaded if you reject or have not answered',
    /nothing is loaded from Google/.test(PRIVACY));
  check('it says the console is not measured', /never inside the Cygenix migration console/.test(PRIVACY));
  check('the cookie table lists both GA cookies',
    /<td>_ga<\/td>/.test(PRIVACY) && /<td>_ga_K3NJP5GX2G<\/td>/.test(PRIVACY));
  check('it says how to withdraw', /withdraw your choice/.test(PRIVACY));

  /* ── 7. The deployment promise ─────────────────────────────────────── */
  // The brief was explicit: adding headers here has broken the site before.
  // The five blocks that were already present are left exactly as they
  // were; what this pins is that shipping analytics added nothing to the
  // deployment configuration — no CSP, no allow-list for Google's host.
  // The module needs none, because no header governs a script the page
  // appends to its own head.
  section('7. netlify.toml gained nothing');
  check('the five header blocks that were there are still there, and no sixth was added',
    (TOML.match(/\[\[headers\]\]/g) || []).length === 5);
  check('NO Content-Security-Policy WAS ADDED — one would have to name Google and it is deliberately absent',
    !/Content-Security-Policy\s*=/.test(TOML));
  check('the deployment config does not mention Google at all', !/googletagmanager|gtag|analytics/i.test(TOML));

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
