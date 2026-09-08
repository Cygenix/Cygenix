// tests/security-section.test.js — the #security section, against the code it
// describes and against the rules it was written under.
//
// The brief that produced this section put one rule above the others: a
// security section with one wrong claim is worth less than no security
// section. That is a maintenance problem, not a writing problem — the section
// was true on the day it shipped, and the code underneath it will be edited
// by people who never read it.
//
// So, as in tests/data-claims.test.js, the claims are pinned in pairs: the
// code still does the thing, and the page still says so. Change the audit
// entry shape, the preflight retention, the Postgres SSL default or the
// Data Stream status, and this file fails on the copy — which is the moment
// somebody should be rewriting the sentence rather than discovering it is
// false on a customer call.
//
// The rest is the brief's acceptance list, in the order it gives it.
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

console.log('Security section — the claims, and the code under them\n');

const index = read('public', 'index.html');
const start = index.indexOf('<section class="section" id="security">');
const sec = start === -1 ? '' : index.slice(start, index.indexOf('</section>', index.indexOf('Assurance</h3>')) + 10);
const text = sec.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');

/* ── 1. It exists, where the brief put it ───────────────────────────────── */

check('the section exists', start !== -1 && sec.length > 4000, sec.length);
check('it sits between Governance and Planning, as specified',
  index.indexOf('id="governance"') < start && start < index.indexOf('id="planning"'));
check('the nav has Security, in the same position',
  /<a href="#governance">Governance<\/a>\s*<a href="#security">Security<\/a>\s*<a href="#planning">Planning<\/a>/.test(index));
// A bare "#security" in the footer would go nowhere from /pricing or /terms,
// where the footer link also appears in spirit; the homepage's own footer uses
// the rooted form so copying it elsewhere keeps working.
check('the footer links Security next to Privacy, by a path that resolves off the homepage',
  /<a href="\/privacy">Privacy<\/a> ·\s*<a href="\/#security">Security<\/a>/.test(index));

/* ── 2. The six blocks, and a valid heading outline ─────────────────────── */

const eyebrows = (sec.match(/<h3 class="sec-eyebrow">([^<]+)<\/h3>/g) || [])
  .map((s) => s.replace(/<[^>]+>/g, ''));
check('six blocks, in the brief\'s order',
  eyebrows.join(' · ') === 'The data path · Sampling · Credentials · Enforcement · Audit · Assurance',
  eyebrows.join(' · '));
check('one h2 for the section and h3 for each block — no level is skipped',
  (sec.match(/<h2/g) || []).length === 1 && (sec.match(/<h3/g) || []).length === 6
  && sec.indexOf('<h2') < sec.indexOf('<h3'));
check('the headline is a declarative, not a noun phrase like "Our Security Commitment"',
  /<h2 class="section-title">Where your data goes, named place by place<\/h2>/.test(sec));

/* ── 3. Design constraints ──────────────────────────────────────────────── */

// tests/landing-theme.test.js already fails the whole page on a stray hex.
// This is the narrower claim: the section introduced none of its own, which
// is what makes it work in both themes.
const styleStart = index.indexOf('/* ── Security section ');
const secCss = index.slice(styleStart, index.indexOf('/* ── Buttons ─', styleStart));
check('the section\'s CSS uses tokens only — no hex, no rgb literal',
  styleStart !== -1 && !/#[0-9a-fA-F]{3,8}\b/.test(secCss) && !/rgba?\(/.test(secCss),
  (secCss.match(/#[0-9a-fA-F]{3,8}\b|rgba?\([^)]*\)/g) || []).slice(0, 3).join(', '));
check('and the markup carries no inline colour of its own either',
  !/style="[^"]*(?:#[0-9a-fA-F]{3,8}|rgba?\()/.test(sec),
  (sec.match(/style="[^"]*(?:#[0-9a-fA-F]{3,8}|rgba?\()[^"]*"/g) || [])[0]);
check('exactly one mock panel, as the brief allows',
  (sec.match(/class="mock reveal"/g) || []).length === 1);
check('no new runtime dependency was added for it',
  !/<script src="https?:/.test(sec) && !/<link[^>]+href="https?:/.test(sec));

/* ── 4. Accessibility ───────────────────────────────────────────────────── */

check('both tables carry a caption and scoped headers, so they read as tables',
  (sec.match(/<caption class="sec-caption">/g) || []).length === 2
  && (sec.match(/<th scope="col">/g) || []).length >= 7
  && (sec.match(/<th scope="row">/g) || []).length >= 14);
// The wide tables scroll inside their own box. A scrollable box that cannot be
// reached by keyboard is a trap for anyone not using a mouse.
// The first cut of this section had the focus STYLE but no tabindex, so the
// box could scroll and never be focused — a column off-screen with no way to
// reach it. The browser smoke caught it; this keeps it caught.
check('a wide table scrolls inside its own box, and that box is focusable and styled for it',
  /\.sec-scroll\{overflow-x:auto/.test(secCss) && /\.sec-scroll:focus-visible\{outline:/.test(secCss)
  && (sec.match(/<div class="sec-scroll" tabindex="0" role="region" aria-label="[^"]+">/g) || []).length === 2,
  (sec.match(/<div class="sec-scroll"[^>]*>/g) || []).join(' | '));
// Every pill in the assurance table says its state in words. Colour alone
// would leave the whole table meaningless to a colour-blind reader.
{
  const pills = sec.match(/<span class="pill p-[a-z]+">([^<]+)<\/span>/g) || [];
  const wordless = pills.filter((p) => !/>[A-Za-z][A-Za-z ]+</.test(p));
  check('every status pill carries a word, never colour alone',
    pills.length >= 10 && wordless.length === 0, wordless.join(' '));
}
check('the links inside the section have a visible focus treatment',
  /\.sec-lede a\{[^}]*border-bottom/.test(secCss) && /\.sec-note a\{[^}]*border-bottom/.test(secCss));

/* ── 5. No badge, no claim ──────────────────────────────────────────────── */

check('the section states plainly that no certification is held',
  /Cygenix holds no security certification/.test(text)
  && /Not\s*ISO 27001, not SOC 2, not Cyber Essentials/.test(text));
check('and invents no roadmap for one',
  /none is in progress with a date we could quote/.test(text));
check('no shield iconography is used to imply one',
  // ic-shield appears once, on the sentence about the audit trail's LIMIT —
  // not next to a certification claim.
  (sec.match(/ic-shield/g) || []).length <= 1);

/* ── 6. Every factual claim, against the code ───────────────────────────── */

const dbConnect = read('netlify', 'functions', 'db-connect.js');
const preflight = read('public', 'cygenix-preflight.js');
const orgStore = read('netlify', 'functions', 'lib', 'org-store.js');
const rbac = read('netlify', 'functions', 'lib', 'rbac.js');
const tenancy = read('netlify', 'functions', 'lib', 'tenancy.js');
const caps = require(path.join(ROOT, 'public', 'cygenix-capabilities.js'));

check('CODE+COPY: preflight keeps three examples per column, and the section says so',
  /f\.examples\.length < 3/.test(preflight) && /localStorage\.setItem\(STORE_KEY/.test(preflight)
  && /up to three offending values per rejecting column/.test(text)
  && /saved in your browser and is not synced to us/.test(text));
check('CODE+COPY: the audit entry has no SQL text, and the section makes that the point',
  /action: evt\.action/.test(orgStore) && !/sql:/.test(orgStore)
  && /none of them is your SQL/.test(text)
  && /No statement text and no column values are written/.test(text));
check('CODE+COPY: verification names where a break starts, not a boolean',
  /return \{ ok: false, brokenAt: e\.seq/.test(rbac)
  && /names the entry where a break starts/.test(text)
  && /broken at entry 862/.test(sec));
check('CODE+COPY: the chain\'s known limit is stated rather than buried',
  // CLAUDE.md requires this caveat wherever tamper-evidence is claimed.
  /audit append lost after retries/.test(orgStore)
  && /no transactions, so two appends at the same instant can race/.test(text)
  && /tamper- evident , not tamper-proof/.test(text.replace(/<[^>]*>/g, '')));
check('CODE+COPY: an approval is bound to a hash of the statement',
  /The hash binds it to the exact statement/.test(tenancy)
  && /bound\s*to a hash of the exact statement it approved/.test(text));
check('CODE+COPY: Postgres can default to no TLS, and the section warns instead of claiming',
  /ssl = cloudHost \? \{ rejectUnauthorized: false \} : false;/.test(dbConnect)
  && /the default is no TLS at all/.test(text));
check('CODE+COPY: Data Stream is preview and holds demo data, so it retains nothing',
  caps.FEATURES.find((f) => f.id === 'data_stream').status === 'preview'
  && /Data Stream holds nothing/.test(text) && /seeded demonstration data/.test(text));
check('CODE+COPY: the AI call leaves the browser with names and types, on the caller\'s key',
  /'x-api-key':apiKey/.test(read('public', 'object-mapping-app.js'))
  && /straight to Anthropic on your API key/.test(text)
  && /no row value is in it/.test(text));
check('CODE+COPY: scheduling stores the connection string, and the section says why',
  /srcConn: srcConn \|\| null/.test(read('netlify', 'functions', 'scheduler.js'))
  && /scheduling stores it on our side, in Cosmos DB/.test(text));
check('CODE+COPY: there is no key vault, so customer-managed keys are not offered',
  !/@azure\/keyvault/.test(read('package.json'))
  && /Customer-managed keys are not offered — there is no key vault/.test(text));

/* The one thing the audit could not find must not appear. The homepage sells
   a validation failure report with chosen columns; it was not in the code, so
   the section says nothing about it either way. */
check('the failure-report feature the audit could not locate is not described here',
  !/failure report/i.test(text),
  'if that feature is found or built, this block needs a sixth sampling surface');

/* ── 7. Security headers, since the section now claims them ─────────────── */

const toml = read('netlify.toml');
check('CONFIG: the headers the section claims are actually set',
  /Strict-Transport-Security = "max-age=31536000; includeSubDomains"/.test(toml)
  && /X-Content-Type-Options = "nosniff"/.test(toml)
  && /X-Frame-Options = "DENY"/.test(toml)
  && /Referrer-Policy = "strict-origin-when-cross-origin"/.test(toml));
check('COPY: and the section claims no more than that',
  /HTTPS only, HSTS, and the usual content-type, framing and referrer headers/.test(text)
  && !/Content-Security-Policy/i.test(text),
  'no CSP is set, so the page must not imply one');
check('CONFIG: DENY is safe because nothing in public/ is ever framed',
  fs.readdirSync(path.join(ROOT, 'public')).filter((f) => f.endsWith('.html'))
    .every((f) => !/<iframe/.test(read('public', f))));

/* ── 8. security.txt ────────────────────────────────────────────────────── */

const stPath = path.join(ROOT, 'public', '.well-known', 'security.txt');
check('security.txt is served from /.well-known/', fs.existsSync(stPath));
const st = fs.readFileSync(stPath, 'utf8');
check('it has the two fields RFC 9116 requires, plus language and canonical',
  /^Contact: mailto:security@cygenix\.co\.uk$/m.test(st)
  && /^Expires: /m.test(st) && /^Preferred-Languages: en$/m.test(st)
  && /^Canonical: https:\/\/cygenix\.co\.uk\/\.well-known\/security\.txt$/m.test(st));
check('and its expiry is still in the future',
  new Date(/^Expires: (.+)$/m.exec(st)[1]) > new Date(),
  'an expired security.txt tells a researcher the contact is stale too');
check('the section points at it',
  /href="\/\.well-known\/security\.txt"/.test(sec));

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
