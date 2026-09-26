#!/usr/bin/env node
/* stamp-assets.js — content-version every local asset URL in public/.
 *
 * THE PROBLEM THIS SOLVES
 * public/*.js and *.css are served with a long, immutable cache life (see
 * netlify.toml), and the HTML that references them with `max-age=0,
 * must-revalidate`. That is the right split ONLY IF every long-lived URL
 * changes whenever its content does. This script is what makes that true.
 *
 * It began as a script-tag stamper, when scripts were cached for five minutes
 * with a day of stale-while-revalidate: a returning browser served its cached
 * copy and fetched the fix in the background, so a deployed fix could take a
 * day to reach someone who already had the page open — which is exactly how a
 * fixed bug kept "still happening". Stamping the <script> tags closed that.
 *
 * Then the cache life went to a year (Sep-2026), because five minutes meant
 * every navigation after that revalidated ~50 unchanged files with the
 * server. A year is only safe if NOTHING long-lived is reached by an
 * unstamped URL, and two things were:
 *
 *   · <link rel="stylesheet" href="/x.css">  — never stamped at all
 *   · scripts injected at runtime            — s.src = '/server-migration.js',
 *     the sidebar's lazy Drive modal, the a11y helper, drive-sync. No tag
 *     to stamp, so they relied on the short header window that no longer
 *     exists.
 *
 * Both are stamped now. The second kind lives INSIDE other files, so the
 * hash of cygenix-sidebar.js depends on the hash of cygenix-drive-modal.js,
 * which is why the JS pass repeats until nothing changes (a leaf's hash is
 * fixed; the file naming it changes once; the page naming THAT changes once).
 *
 * WHAT IT DOES
 *   src="/foo.js"          →  src="/foo.js?v=<10 hex of sha256(foo.js)>"
 *   href="/foo.css"        →  href="/foo.css?v=…"
 *   '/foo.js' in JS/HTML   →  '/foo.js?v=…'      (a quoted root-relative .js)
 * Idempotent: re-running restamps in place. A stale stamp is replaced, never
 * appended. A path with no file on disk is left alone rather than guessed.
 *
 * WHERE IT RUNS
 * netlify.toml's build command, so a deploy can never ship mismatched stamps.
 * Also by hand (`node scripts/stamp-assets.js`) and in --check mode, which
 * reports drift without writing — that is what the test suite calls.
 *
 * NOT COVERED
 * Fonts. They are referenced from CSS (@font-face) and from <link
 * rel="preload">, and have no header rule of their own, so they revalidate;
 * a changed font should be given a new file name.
 */
'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PUBLIC = path.join(__dirname, '..', 'public');

// src="/thing.js" or src="/thing.js?v=abc" — local, root-relative, no host.
const SCRIPT_RE = /(<script\b[^>]*\bsrc=")\/([A-Za-z0-9._-]+\.js)(\?v=[A-Za-z0-9]+)?(")/g;
// href="/thing.css" on a <link>, same rules.
const LINK_RE = /(<link\b[^>]*\bhref=")\/([A-Za-z0-9._-]+\.css)(\?v=[A-Za-z0-9]+)?(")/g;
// A quoted root-relative script path: the form every runtime injection uses
// (s.src = '/x.js', inject('id', '/x.js')). Whole literal only, same quote
// both ends, so a longer URL or a path fragment is never touched.
const INJECT_RE = /(['"])\/([A-Za-z0-9._-]+\.js)(\?v=[A-Za-z0-9]+)?\1/g;

function hashOfText(text) {
  return crypto.createHash('sha256').update(text).digest('hex').slice(0, 10);
}
function hashOf(file) {
  return hashOfText(fs.readFileSync(file));
}

function rewrite(html, re, hashFor, build, missing) {
  let changed = false;
  const out = html.replace(re, (full, pre, file, oldQ, post) => {
    const h = hashFor(file);
    if (!h) { missing.push(file); return full; }          // not ours — leave alone
    const next = build(pre, file, h, post);
    if (next !== full) changed = true;
    return next;
  });
  return { out, changed };
}

/** Stamp one HTML string. Returns { out, changed, missing[] }. */
function stampHtml(html, hashFor) {
  const missing = [];
  let changed = false;
  let out = html;
  for (const [re, build] of [
    [SCRIPT_RE, (pre, f, h, post) => pre + '/' + f + '?v=' + h + post],
    [LINK_RE,   (pre, f, h, post) => pre + '/' + f + '?v=' + h + post],
    [INJECT_RE, (q, f, h)         => q + '/' + f + '?v=' + h + q],
  ]) {
    const r = rewrite(out, re, hashFor, build, missing);
    out = r.out; changed = changed || r.changed;
  }
  return { out, changed, missing };
}

/** Stamp one JS string: only the injection literals.
 *
 * `self` is the file's own name. Five modules quote their own path in a
 * header comment ("add <script src="/cygenix-sidebar.js"> to every page"),
 * and a file cannot carry a stamp for itself: stamping it changes its hash,
 * which changes the stamp, which changes the hash — the pass never settles.
 * A file's own name is therefore left exactly as written. */
function stampJs(js, hashFor, self) {
  const missing = [];
  const guarded = (file) => (file === self ? null : hashFor(file));
  const r = rewrite(js, INJECT_RE, guarded, (q, f, h) => q + '/' + f + '?v=' + h + q, missing);
  return { out: r.out, changed: r.changed, missing: missing.filter((m) => m !== self) };
}

function run({ check = false } = {}) {
  const names = fs.readdirSync(PUBLIC);
  const jsNames = names.filter((n) => n.endsWith('.js'));

  // In-memory copies of the JS files: a pass may change one, and the next
  // pass must hash what it WOULD be, not what is on disk.
  const js = new Map(jsNames.map((n) => [n, fs.readFileSync(path.join(PUBLIC, n), 'utf8')]));
  const original = new Map(js);

  const hashFor = (file) => {
    if (js.has(file)) return hashOfText(js.get(file));
    const p = path.join(PUBLIC, file);
    return fs.existsSync(p) ? hashOf(p) : null;
  };

  // JS pass, repeated to a fixed point. Bounded: a cycle (A injects B injects
  // A) can never settle, and would be a bug worth failing the build for.
  for (let pass = 0; pass < 6; pass++) {
    let moved = false;
    for (const n of jsNames) {
      const r = stampJs(js.get(n), hashFor, n);
      if (r.changed) { js.set(n, r.out); moved = true; }
    }
    if (!moved) break;
    if (pass === 5) {
      console.error('stamp-assets: injection stamps did not settle — is there a cycle of runtime injections?');
      return 1;
    }
  }

  const drift = [];
  let stamped = 0;
  for (const n of jsNames) {
    if (js.get(n) === original.get(n)) continue;
    drift.push(n);
    if (!check) { fs.writeFileSync(path.join(PUBLIC, n), js.get(n)); stamped++; }
  }
  for (const name of names) {
    if (!name.endsWith('.html')) continue;
    const p = path.join(PUBLIC, name);
    const { out, changed } = stampHtml(fs.readFileSync(p, 'utf8'), hashFor);
    if (!changed) continue;
    drift.push(name);
    if (!check) { fs.writeFileSync(p, out); stamped++; }
  }

  if (check) {
    if (drift.length) {
      console.error('stamp-assets: ' + drift.length + ' file(s) have stale asset stamps:');
      drift.forEach((d) => console.error('  ' + d));
      console.error('Run: node scripts/stamp-assets.js');
      return 1;
    }
    console.log('stamp-assets: all asset stamps current');
    return 0;
  }
  console.log('stamp-assets: stamped ' + stamped + ' file(s)');
  return 0;
}

if (require.main === module) {
  process.exit(run({ check: process.argv.includes('--check') }));
}

module.exports = { stampHtml, stampJs, hashOf, hashOfText, run, SCRIPT_RE, LINK_RE, INJECT_RE };
