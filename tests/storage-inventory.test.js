// tests/storage-inventory.test.js — the inventory cannot go stale, and a key
// cannot be added without somebody deciding what it is.
//
// WI-1's first acceptance criterion is a document listing every client-side
// storage key with its class. A hand-maintained list is accurate on the day it
// is written; this one is generated from the source and checked here, which is
// the same argument WI-9 makes about capability claims.
//
// The check that earns its place is the last one: a NEW key that nobody has
// classified fails the build. Without it the document decays silently, and the
// decay is invisible precisely because the document still looks complete.
'use strict';

const fs = require('fs');
const path = require('path');
const inv = require('../scripts/storage-inventory.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};

console.log('Storage inventory — every key classified, the document current\n');

const DOC = path.join(__dirname, '..', 'docs', 'storage-inventory.md');

check('the inventory document exists', fs.existsSync(DOC),
  'run node scripts/storage-inventory.js');

if (fs.existsSync(DOC)) {
  check('the committed document is exactly what the generator produces',
    fs.readFileSync(DOC, 'utf8') === inv.render(),
    're-run node scripts/storage-inventory.js and commit the result');
}

const { rows, unclassified } = inv.build();

check('every storage key in the client is classified',
  unclassified.length === 0,
  unclassified.length
    ? 'classify these in scripts/storage-inventory.js: '
      + unclassified.map(u => u.key + ' (' + u.files.join(', ') + ')').join('; ')
    : '');

check('the scan actually found the codebase', rows.length > 40, String(rows.length));

/* Every class is represented, so a scanner that silently stopped matching
   would be caught rather than reported as "nothing to see". */
for (const c of ['A', 'B', 'C']) {
  check('class ' + c + ' has entries', rows.some(r => r.cls === c));
}

/* Class A is a promise: losing it loses customer work. Each entry must say
   what its server source of truth is — "" would mean nobody has decided. */
{
  const silent = rows.filter(r => r.cls === 'A' && !r.note);
  check('every authoritative key states its server source of truth (or says it has none)',
    silent.length === 0, silent.map(r => r.key).join(', '));
}

/* The secrets list is a finding list. It must not grow without a decision:
   pin the count, so a new credential in browser storage fails here. */
{
  const secrets = rows.filter(r => r.cls === 'S');
  check('the credential-in-browser-storage findings are the known set of 7, no more',
    secrets.length <= 7,
    'new credential-shaped key(s) in browser storage: ' + secrets.map(r => r.key).join(', '));
  check('and each one names the work item that removes it',
    secrets.every(r => /WI-10|clear on consume|proxy/i.test(r.note || '')),
    secrets.filter(r => !/WI-10|clear on consume|proxy/i.test(r.note || '')).map(r => r.key).join(', '));
}

/* The keys the sync engine declares must be the keys the inventory calls
   synced. These two lists drifting apart is how a Class A key ends up
   local-only without anyone noticing. */
{
  const sync = fs.readFileSync(path.join(__dirname, '..', 'public', 'cygenix-cosmos-sync.js'), 'utf8');
  // The array carries explanatory comments that themselves quote strings, so
  // strip comment lines before reading the keys out of it.
  const block = ((sync.match(/const SYNC_KEYS = \[([\s\S]*?)\];/) || [])[1] || '')
    .split('\n').filter(l => !/^\s*\/\//.test(l)).join('\n');
  const declared = new Set([...block.matchAll(/'(cygenix_[a-z_]+)'/g)].map(m => m[1]));
  check('the sync engine declares a key list', declared.size > 10, String(declared.size));

  // Read from the classification table, not from the scan: several synced keys
  // are handled generically by the sync engine and never named in a
  // localStorage call, so they would not appear in a scan of call sites.
  const claimed = Object.keys(inv.CLASSES)
    .filter(k => /synced \(SYNC_KEYS\)/.test((inv.CLASSES[k] || [])[2] || ''));
  const lying = claimed.filter(k => !declared.has(k));
  check('every key the inventory calls synced really is in SYNC_KEYS',
    lying.length === 0, lying.join(', '));

  const missed = [...declared].filter(k => !claimed.includes(k));
  check('and every key in SYNC_KEYS is recorded as synced in the inventory',
    missed.length === 0, missed.join(', '));
}

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
