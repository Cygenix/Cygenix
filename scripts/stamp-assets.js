#!/usr/bin/env node
/* stamp-assets.js — content-version every local <script src> in public/*.html.
 *
 * THE PROBLEM THIS SOLVES
 * public/*.js is served with `max-age=300, stale-while-revalidate=86400`, and
 * the HTML that references it with `max-age=0, must-revalidate`. So the HTML
 * is always fresh but the URL it points at is unversioned — a returning
 * browser serves its CACHED copy of the script (stale, for up to a day) and
 * only fetches the new one in the background. A deployed fix could therefore
 * take a day to reach someone who already had the page open, which is exactly
 * how a fixed bug kept "still happening".
 *
 * That was tolerable when each page carried its logic inline (inline script is
 * part of the always-revalidated HTML). Once the big pages moved their code to
 * external files it stopped being tolerable.
 *
 * WHAT IT DOES
 * Rewrites  src="/foo.js"  →  src="/foo.js?v=<first 10 of sha256(foo.js)>".
 * The HTML is always revalidated, so a changed file means a changed URL means
 * the browser must fetch it — no stale window at all. An unchanged file keeps
 * its URL and stays cached. Idempotent: re-running restamps in place.
 *
 * WHERE IT RUNS
 * netlify.toml's build command, so a deploy can never ship mismatched stamps.
 * Also runnable by hand (`node scripts/stamp-assets.js`) and in --check mode,
 * which reports drift without writing — that is what the test suite calls.
 *
 * NOT COVERED
 * Scripts injected at runtime by other scripts (the sidebar's lazy Drive
 * modal, for instance) have no HTML tag to stamp. Those still rely on the
 * cache headers, which is why the stale-while-revalidate window is kept
 * short rather than a day.
 */
'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PUBLIC = path.join(__dirname, '..', 'public');

// src="/thing.js" or src="/thing.js?v=abc" — local, root-relative, no host.
const SCRIPT_RE = /(<script\b[^>]*\bsrc=")\/([A-Za-z0-9._-]+\.js)(\?v=[A-Za-z0-9]+)?(")/g;

function hashOf(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex').slice(0, 10);
}

/** Stamp one HTML string. Returns { out, changed, missing[] }. */
function stampHtml(html, hashFor) {
  const missing = [];
  let changed = false;
  const out = html.replace(SCRIPT_RE, (full, pre, file, oldQ, post) => {
    const h = hashFor(file);
    if (!h) { missing.push(file); return full; }          // not ours — leave alone
    const next = pre + '/' + file + '?v=' + h + post;
    if (next !== full) changed = true;
    return next;
  });
  return { out, changed, missing };
}

function run({ check = false } = {}) {
  const cache = new Map();
  const hashFor = (file) => {
    if (cache.has(file)) return cache.get(file);
    const p = path.join(PUBLIC, file);
    const h = fs.existsSync(p) ? hashOf(p) : null;
    cache.set(file, h);
    return h;
  };

  const drift = [];
  let stamped = 0;
  for (const name of fs.readdirSync(PUBLIC)) {
    if (!name.endsWith('.html')) continue;
    const p = path.join(PUBLIC, name);
    const html = fs.readFileSync(p, 'utf8');
    const { out, changed } = stampHtml(html, hashFor);
    if (!changed) continue;
    drift.push(name);
    if (!check) { fs.writeFileSync(p, out); stamped++; }
  }

  if (check) {
    if (drift.length) {
      console.error('stamp-assets: ' + drift.length + ' page(s) have stale script stamps:');
      drift.forEach(d => console.error('  ' + d));
      console.error('Run: node scripts/stamp-assets.js');
      return 1;
    }
    console.log('stamp-assets: all script stamps current');
    return 0;
  }
  console.log('stamp-assets: stamped ' + stamped + ' page(s)');
  return 0;
}

if (require.main === module) {
  process.exit(run({ check: process.argv.includes('--check') }));
}

module.exports = { stampHtml, hashOf, run, SCRIPT_RE };
