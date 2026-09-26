// tests/page-load.test.js — what a console page makes the browser wait for.
//
// WHY THIS FILE EXISTS
//
// A page-load review (Sep-2026) measured the console at 5 Mbps on a mid-range
// laptop and found three things that nothing else in the suite could see:
//
//   1. The five Data Stream screens stopped parsing a tenth of the way in.
//      Each carried its code as a 60KB inline <script> right after the
//      engine modules it reads, and an inline script cannot be deferred, so
//      the modules could not be either. 182–272KB of JavaScript was fetched
//      and run before the markup that IS the screen had even been parsed.
//   2. MSAL was fetched from two different CDNs depending on the page, so a
//      browser that already held it downloaded it again on the next screen.
//   3. No page told the browser which font files it would need. They were
//      discovered only after the stylesheet had arrived and been parsed, so
//      text painted in a fallback face and swapped a moment later.
//
// All three are properties of the HTML, and all three regress silently: a
// new inline block, a copied <script> tag, a page built from an older
// template. This pins the shape that was measured to be fast.

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
const pages = fs.readdirSync(PUB).filter((f) => f.endsWith('.html')).sort();
const consolePages = pages.filter((f) => /cygenix-console\.css/.test(read(f)));

// Every external script tag on a page, with where it sits and whether it
// blocks the parser. `defer`/`async` may come before OR after src.
function scripts(html) {
  const headEnd = html.indexOf('</head>');
  const out = [];
  const re = /<script\b([^>]*)\bsrc="([^"?]+)[^"]*"([^>]*)>/g;
  let m;
  while ((m = re.exec(html))) {
    const attrs = m[1] + m[3];
    out.push({
      src: m[2], at: m.index, pct: Math.floor(100 * m.index / html.length),
      head: m.index < headEnd,
      blocking: !/\bdefer\b|\basync\b/.test(attrs),
    });
  }
  return out;
}

(async () => {
  console.log('Page load — what the parser waits for\n');

  /* ── 1. The parser is not stopped early ───────────────────────────────── */
  section('1. No console page stops parsing early for a script');

  // The first blocking script in the BODY, as a percentage of the way through
  // the page. Scripts at the very end (98–99%) block nothing that matters:
  // the markup is already parsed. Scripts at 7–14% did.
  const early = [];
  for (const f of consolePages) {
    const first = scripts(read(f)).find((s) => !s.head && s.blocking);
    if (first && first.pct < 90) early.push(f + ' @' + first.pct + '% (' + first.src + ')');
  }
  // sql-editor.html loads cygenix-sql-windows.js (11KB) at two thirds of the
  // way through; the third that follows is 17KB of markup. Known, small, and
  // listed here so it cannot be joined by anything larger unnoticed.
  const KNOWN = ['sql-editor.html'];
  check('THE ONLY PAGE THAT BLOCKS BEFORE 90% IS THE ONE LISTED HERE',
    early.every((e) => KNOWN.some((k) => e.startsWith(k))), early.join(', '));

  const STREAM = ['data_stream', 'data_stream_designer', 'data_stream_events', 'data_stream_monitor', 'data_stream_store'];
  for (const p of STREAM) {
    const html = read(p + '.html');
    const app = p.replace(/_/g, '-') + '-app.js';
    const tags = scripts(html);
    const appTag = tags.find((s) => s.src === '/' + app);
    check(p + ' carries its code in ' + app + ', deferred',
      !!appTag && !appTag.blocking, appTag ? 'blocking' : 'no tag');
    check('  …and the file exists, is strict, and declares the screen\'s functions at top level',
      fs.existsSync(path.join(PUB, app)) && /^'use strict';/m.test(read(app)) && /^function ds/m.test(read(app)));
    // The engine modules it reads must come before it and be deferred too:
    // deferred scripts run in document order, so this is the contract that
    // keeps the screen finding its globals.
    const need = ['/cygenix-datastream.js', '/cygenix-datastream-ui.js', '/cygenix-datastream-page.js'];
    check('  …after the three engine modules, all deferred',
      need.every((n) => { const t = tags.find((s) => s.src === n); return t && !t.blocking && t.at < appTag.at; }));
    const first = tags.find((s) => !s.head && s.blocking);
    // The eight shared modules (busy, model, page-reader, assistant, tour…)
    // still sit at the very end as blocking tags; with the screen's code
    // gone the page is shorter, so "the very end" is 91–93% now.
    check('  …and nothing blocks the parser before 90% of the page',
      !first || first.pct >= 90, first ? first.pct + '% ' + first.src : '');
    // No 60KB inline block may quietly come back.
    const inline = [...html.matchAll(/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/g)].map((m) => m[1].length);
    check('  …and no inline script on the page is over 4KB',
      inline.every((n) => n < 4096), inline.filter((n) => n >= 4096).join(', '));
  }
  check('the Designer also defers the three connection modules its form reads',
    ['/connections.js', '/cygenix-saved-conn-secrets.js', '/cygenix-stream-destinations.js']
      .every((n) => { const t = scripts(read('data_stream_designer.html')).find((s) => s.src === n); return t && !t.blocking; }));

  /* ── 2. One copy of MSAL ──────────────────────────────────────────────── */
  section('2. MSAL comes from one place');

  const msal = new Set();
  for (const f of pages) for (const m of read(f).matchAll(/src="(https?:[^"]*msal-browser[^"]*)"/g)) msal.add(m[1]);
  check('EVERY PAGE THAT LOADS MSAL LOADS THE SAME URL, so the second page hits the cache',
    msal.size === 1, [...msal].join(' | '));
  check('and it is the one the sign-in page uses, so it is already held by the time the console opens',
    [...msal][0] === (read('login.html').match(/src="(https?:[^"]*msal-browser[^"]*)"/) || [])[1]);

  /* ── 3. The fonts are announced ───────────────────────────────────────── */
  section('3. The first paint\'s fonts are preloaded');

  const missing = consolePages.filter((f) => {
    const s = read(f);
    return !/<link rel="preload" href="\/fonts\/noto-sans-400-latin\.woff2" as="font" type="font\/woff2" crossorigin>/.test(s)
      || !/<link rel="preload" href="\/fonts\/noto-sans-600-latin\.woff2" as="font" type="font\/woff2" crossorigin>/.test(s);
  });
  check('EVERY CONSOLE PAGE PRELOADS THE 400 AND 600 WEIGHTS', missing.length === 0, missing.join(', '));
  check('and only those two — preloading every weight would cost more bandwidth than it saves',
    consolePages.every((f) => (read(f).match(/rel="preload" href="\/fonts\//g) || []).length === 2));
  check('the preloads come before the stylesheet, so they are the first thing after the HTML',
    consolePages.every((f) => { const s = read(f); return s.indexOf('rel="preload" href="/fonts/') < s.indexOf('href="/cygenix-console.css"'); }));
  check('the files they name exist', ['noto-sans-400-latin.woff2', 'noto-sans-600-latin.woff2']
    .every((f) => fs.existsSync(path.join(PUB, 'fonts', f))));
  check('and carry crossorigin, without which the browser fetches every font twice',
    consolePages.every((f) => {
      const tags = read(f).match(/<link rel="preload" href="\/fonts\/[^>]*>/g) || [];
      return tags.length === 2 && tags.every((t) => /\bcrossorigin\b/.test(t));
    }));

  /* ── 4. The stamp survives the attribute ──────────────────────────────── */
  section('4. Content stamps still apply');

  // SCRIPT_RE is global (/g), so .test() is stateful across calls; a fresh
  // copy per string is the honest way to ask "does this match".
  const { SCRIPT_RE } = require('../scripts/stamp-assets.js');
  const matches = (str) => new RegExp(SCRIPT_RE.source).test(str);
  check('a deferred tag is still matched by the stamper, so the new files are versioned',
    matches('<script src="/data-stream-app.js" defer></script>')
    && matches('<script defer src="/data-stream-app.js"></script>'));
  check('and the five page apps are stamped on their pages',
    STREAM.every((p) => new RegExp(p.replace(/_/g, '-') + '-app\\.js\\?v=[a-f0-9]{10}').test(read(p + '.html'))));

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
