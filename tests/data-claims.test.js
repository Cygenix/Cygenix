// tests/data-claims.test.js — what the site says about your data, against
// what the code does with it.
//
// Every claim pinned here was WRONG on the live site until the commit that
// added this file, and each was wrong in the same direction: it promised less
// exposure than exists. The homepage said the product connected to your
// databases from the browser; the privacy policy said connection strings were
// never transmitted to our servers and that we collect only table and column
// names. None of that was true. docs/security-posture-audit.md has the trace.
//
// The reason this file exists rather than a careful re-read every few months:
// these claims are about behaviour in code that is edited for other reasons.
// A copy assertion alone would go stale silently — it would still pass while
// the sentence it guards quietly became false again.
//
// So each check below is TWO assertions: the code still does the thing, and
// the page still says so. Change the code and the test fails on the copy,
// which is the moment someone should be rewriting it.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

console.log('Data claims — the site against the code\n');

const index = read('public', 'index.html');
const privacy = read('public', 'privacy.html');
const pricing = read('public', 'pricing.html');
const terms = read('public', 'terms.html');
const about = read('public', 'about.html');
const llms = read('public', 'llms.txt');
const PUBLIC_COPY = { 'index.html': index, 'privacy.html': privacy, 'pricing.html': pricing,
  'terms.html': terms, 'about.html': about, 'llms.txt': llms };

/* ── 1. Who opens the database connection ───────────────────────────────── */
//
// A browser cannot open a TDS or libpq socket. db-connect.js does it, with the
// mssql and pg drivers, inside a Netlify Function.

const dbConnect = read('netlify', 'functions', 'db-connect.js');
check('CODE: the connection is opened by a Cygenix function, not the browser',
  /require\('mssql'\)/.test(dbConnect) && /require\('pg'\)/.test(dbConnect),
  'if the drivers ever leave this file, every claim below needs re-reading');
check('CODE: the browser posts queries to it rather than connecting itself',
  /\/\.netlify\/functions\/db-connect/.test(read('public', 'project-builder-app.js')));

// The exact sentence that was wrong, and every phrasing of it.
const DIRECT = /connects? (?:to your databases )?directly|connects directly to your databases|runs in the browser and connects/i;
const offenders = Object.entries(PUBLIC_COPY).filter(([, src]) => DIRECT.test(src)).map(([f]) => f);
check('COPY: no public page still says the browser connects to your databases directly',
  offenders.length === 0, offenders.join(', '));
check('COPY: the homepage says where the connection is actually opened',
  /the database connections are opened\s*\n?\s*by Cygenix, so your rows pass through our service during a run and are not stored there/.test(index));
check('COPY: and so do the terms, which are the binding version',
  /Database connections are opened by Cygenix-operated services rather than by your browser/.test(terms));
check('COPY: and llms.txt, which assistants quote verbatim',
  /the database connections are opened by\s*\n?>?\s*Cygenix/.test(llms));

/* ── 2. Connection strings: the scheduling exception ────────────────────── */
//
// A scheduled job runs with no browser open, so it reads the connection
// string from somewhere. It reads it from the schedule record, in Cosmos.

const scheduler = read('netlify', 'functions', 'scheduler.js');
const runner = read('azure-function', 'src', 'run-migration.js');
check('CODE: a schedule record carries srcConn and tgtConn',
  /srcConn: srcConn \|\| null/.test(scheduler) && /schedule\.tgtConn/.test(runner),
  'this is why "never transmitted to our servers" was false');
check('COPY: the privacy policy no longer claims connection strings never reach us',
  !/are never transmitted to our servers/.test(privacy));
check('COPY: it names scheduling as the exception, and says where they go',
  /scheduling a job stores its source and target connection strings on our servers/.test(privacy)
  && /Azure Cosmos DB/.test(privacy));
check('COPY: and offers Entra as the way to store no password at all',
  /Microsoft Entra ID\s*\n?\s*authentication where your databases support it and no SQL password is stored at all/.test(privacy));

/* ── 3. Database contents ───────────────────────────────────────────────── */

check('COPY: the privacy policy no longer claims we collect only table and column names',
  !/only the schema metadata \(table names and column names\)/.test(privacy));
check('COPY: it says rows pass through, and what is kept instead',
  /rows are read from your source/.test(privacy)
  && /We do not store them\s*\n?\s*there/.test(privacy)
  && /schema metadata, column mappings, row counts, validation findings, conversion\s*\n?\s*reports and job history/.test(privacy));

/* ── 4. Preflight keeps example values ──────────────────────────────────── */

const preflight = read('public', 'cygenix-preflight.js');
check('CODE: preflight retains up to three example values per failing column',
  /f\.examples\.length < 3/.test(preflight) && /f\.examples\.push\(/.test(preflight));
check('CODE: and saves that report to browser storage, not to ours',
  /localStorage\.setItem\(STORE_KEY/.test(preflight)
  && !/cygenix_preflight_results/.test(read('public', 'cygenix-cosmos-sync.js')),
  'if this key is ever synced, the copy below becomes false');
check('COPY: the privacy policy discloses it, and says where it is held',
  /A preflight keeps up to three example values from each column it/.test(privacy)
  && /held in your own browser and is not sent to us/.test(privacy));

/* ── 5. Error messages can quote a value, and are not redacted ──────────── */

check('CODE: there is still no redaction of driver error text',
  !/redact/i.test(dbConnect),
  'if a redactor lands, the privacy policy paragraph about error messages should change');
check('COPY: the privacy policy says so plainly rather than omitting it',
  /A database driver error can quote the value that caused it/.test(privacy)
  && /We do not\s*\n?\s*redact them today/.test(privacy));

/* ── 6. The AI mapping call ─────────────────────────────────────────────── */
//
// The one claim that was understated. The call goes from the browser straight
// to Anthropic on the user's own key, carrying names and types.

const om = read('public', 'object-mapping-app.js');
check('CODE: the mapping call is made from the browser, direct to Anthropic',
  /fetch\('https:\/\/api\.anthropic\.com\/v1\/messages'/.test(om)
  && /anthropic-dangerous-direct-browser-access/.test(om));
check('CODE: on the caller\'s own key, with no Cygenix key substituted',
  /'x-api-key':apiKey/.test(om)
  && /Cygenix has no key of its own to substitute/.test(read('netlify', 'functions', 'data-proxy.js')));
check('COPY: the privacy policy states the payload and the path',
  /carries column names and types only, never row values/.test(privacy)
  && /It does not pass through Cygenix/.test(privacy));
check('COPY: and the homepage no longer implies the model sees sampled values',
  /The scoring runs on your machine — the model\s*\n?\s*is sent column names and types, never a value/.test(index));

/* ── 7. Residency, split by what sits where ─────────────────────────────── */

check('COPY: no page still claims data simply stays in the UK',
  !/DATA STAYS IN UK SOUTH/.test(pricing)
  && !/connections are made directly from our UK infrastructure/.test(pricing));
check('COPY: the pricing answer names both regions and which data is in each',
  /stored in Azure Cosmos DB in UK South/.test(pricing)
  && /hosted on Netlify, whose processing is in the US/.test(pricing));
check('COPY: the comparison row promises metadata residency, which is the true claim',
  /<tr><td>UK metadata residency<\/td>/.test(pricing));
check('COPY: the privacy policy splits it the same way',
  /metadata is stored in Microsoft Azure Cosmos DB in the UK South region/.test(privacy)
  && /hosted on Netlify and process in the US/.test(privacy));
check('COPY: and the homepage meta description no longer makes the coarse claim',
  !/UK-hosted/.test(index));
// The pricing page's structured data is generated from the visible answers, so
// a corrected answer that did not reach the markup would be the exact
// mismatch tests/seo.test.js exists to catch. Belt and braces: check here too.
check('COPY: the corrected residency answer reached the structured data',
  /hosted on Netlify, whose processing is in the US/.test(
    (/<script type="application\/ld\+json">([\s\S]*?)<\/script>/.exec(pricing) || ['', ''])[1]));

/* ── 8. Sign-in ─────────────────────────────────────────────────────────── */

check('CODE: sign-in is Entra External ID, verified server-side',
  /cygenix\.ciamlogin\.com/.test(read('public', 'cygenix-auth-token.js'))
  && /jwks/i.test(read('netlify', 'functions', 'lib', 'entra-auth.js')));
check('COPY: the privacy policy says Entra, not Netlify Identity',
  /Sign-in is handled by Microsoft Entra External ID/.test(privacy)
  && !/Authentication is handled by Netlify Identity/.test(privacy));

/* ── 9. Outbound TLS is described as it behaves ─────────────────────────── */

check('CODE: Postgres still defaults to no TLS off a recognised cloud host',
  /ssl = cloudHost \? \{ rejectUnauthorized: false \} : false;/.test(dbConnect));
check('COPY: the privacy policy warns about it instead of claiming TLS everywhere',
  !/All data in transit is encrypted using TLS 1\.2 or higher/.test(privacy)
  && /the default is no TLS at all/.test(privacy)
  && /Certificate validation can be turned off there/.test(privacy));

/* ── 10. Tenancy, named rather than hand-waved ──────────────────────────── */

check('CODE: projects are a store per tenant; Cosmos is partitioned per user',
  /projectStoreName/.test(read('netlify', 'functions', 'projects.js'))
  && /partitionKey: userId/.test(read('azure-function', 'src', 'index.js')));
check('COPY: the privacy policy names both mechanisms rather than saying "isolated"',
  /separate store per organisation/.test(privacy) && /partitioned per user/.test(privacy));

/* ── 11. No certification claim crept in ────────────────────────────────── */
//
// There is no ISO 27001, SOC 2 or Cyber Essentials. Nothing added while
// correcting these claims may imply otherwise.

// Naming a certification is not claiming it: the security section names all
// three in order to say Cygenix holds none, which is the block that makes the
// rest of the section believable. So the rule is not "never mention" — it is
// that every mention sits inside a denial. Same shape as the qualifier window
// in tests/capabilities.test.js.
const CERTS = /ISO ?27001|SOC ?2|Cyber Essentials/gi;
const DENIAL = /\b(no|not|none|neither|nor|without|holds no|does not hold|lacks)\b/i;
const claimed = [];
for (const [f, src] of Object.entries(PUBLIC_COPY)) {
  const text = src.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');
  for (const m of text.matchAll(CERTS)) {
    const before = text.slice(Math.max(0, m.index - 140), m.index);
    if (!DENIAL.test(before)) claimed.push(f + ': …' + before.slice(-60) + '[' + m[0] + ']');
  }
}
check('COPY: every mention of a certification is a denial of holding it, never a claim',
  claimed.length === 0, claimed.slice(0, 3).join(' | '));
// These have no honest use on a page like this. The brief bans them outright
// and "ENTERPRISE-GRADE" was on the pricing trust strip until this work.
const PUFF = /enterprise-grade|bank-level|military-grade|best-in-class/i;
const puffed = Object.entries(PUBLIC_COPY).filter(([, s]) => PUFF.test(s)).map(([f]) => f);
check('COPY: and no page substitutes an assurance adjective for one', puffed.length === 0, puffed.join(', '));

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
