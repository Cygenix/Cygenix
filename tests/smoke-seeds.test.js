// tests/smoke-seeds.test.js — what a browser smoke puts in localStorage is real.
//
// WHY THIS FILE EXISTS
//
// Every browser smoke seeds a signed-in browser before it opens a console
// page: a token, a user, a tier, a cookie-consent answer. Twenty of them
// seeded the consent as the bare string 'all'. cookie-consent.js JSON-parses
// that record and treats anything unparseable as "no answer yet", so the
// banner showed on every run of every one of them — and one check in
// page-reader.smoke.js, which counts controls, failed about one run in forty
// when the banner's four buttons landed between its two snapshots (Sep-2026).
//
// The other nineteen never noticed, which is the point: a seed that does not
// match what the module reads is a smoke running against a page the user
// never sees, and nothing tells you. This pins the seeds to the shape the
// module writes, so the copy-and-paste that spread the string cannot spread
// it again.

'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const DIR = path.join(__dirname, 'browser');
const smokes = fs.readdirSync(DIR).filter((f) => f.endsWith('.smoke.js')).sort();
const consentSrc = fs.readFileSync(path.join(__dirname, '..', 'public', 'cookie-consent.js'), 'utf8');
const VERSION = (consentSrc.match(/CONSENT_VERSION\s*=\s*'(\d+)'/) || [])[1];

console.log('Smoke seeds — the consent record\n');
check('cookie-consent.js declares a CONSENT_VERSION to match against', !!VERSION, 'not found');

const SEED = /localStorage\.setItem\(\s*'cygenix_cookie_consent'\s*,\s*([^;]*?)\);/g;
let seeded = 0;
for (const f of smokes) {
  const src = fs.readFileSync(path.join(DIR, f), 'utf8');
  let m;
  while ((m = SEED.exec(src))) {
    seeded++;
    const value = m[1].trim();
    const asRecord = /^JSON\.stringify\(\s*\{[\s\S]*version:\s*'(\d+)'[\s\S]*essential:\s*true[\s\S]*\}\s*\)$/.exec(value);
    check(f + ' seeds the consent as the record the module writes, version ' + VERSION,
      !!asRecord && asRecord[1] === VERSION, value.slice(0, 80));
  }
}
check('at least the twenty seeds that were swept are still seen by this scan', seeded >= 20, String(seeded));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
