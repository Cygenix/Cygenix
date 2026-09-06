// tests/seo.test.js — what the site tells machines about itself.
//
// robots.txt, sitemap.xml, llms.txt and four blocks of JSON-LD. None of it is
// visible, none of it can break a render, and that is exactly why it rots
// unnoticed: a price that moved, a company number that was corrected in the
// footer, a page that was renamed, an FAQ answer that was reworded. Nobody
// looks at structured data again after the day it ships.
//
// So this file does not check that the markup EXISTS. It checks that every
// fact in it still agrees with the one place that fact actually lives:
//
//   * the sitemap's addresses against the pages on disk and the generated
//     routing table — a sitemap advertising a 404 is worse than none;
//   * the Organization's registered particulars against the footer, which is
//     the legal statement of record;
//   * the AggregateOffer's price range against TIER_PRICES on the pricing
//     page, which is what a visitor is actually quoted;
//   * every FAQ answer against the visible accordion, character for
//     character — markup that does not match visible content is a manual
//     action risk, which is the one way this can actively hurt;
//   * the @id references between the three pages, so the graph joins up
//     rather than describing three unrelated things.
'use strict';

const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const ROOT = path.join(__dirname, '..');
const PUB = path.join(ROOT, 'public');
const read = (f) => fs.readFileSync(path.join(PUB, f), 'utf8');
const routes = require(path.join(ROOT, 'scripts', 'build-routes.js'));

console.log('SEO — what the site tells machines, and whether it is still true\n');

const index = read('index.html');
const pricing = read('pricing.html');
const about = read('about.html');

/* ── 1. robots.txt ──────────────────────────────────────────────────────── */

check('robots.txt is served from the publish root', fs.existsSync(path.join(PUB, 'robots.txt')));
const robots = read('robots.txt');
check('it lets crawlers have the marketing site', /^User-agent: \*$/m.test(robots) && /^Allow: \/$/m.test(robots));
// Disallow is a prefix match, so /login also covers /login.html — the OAuth
// callback address, which is the one .html address the site still serves.
check('and keeps the console and the sign-in wall out of the index',
  /^Disallow: \/dashboard$/m.test(robots) && /^Disallow: \/login$/m.test(robots));
check('it points at the sitemap by absolute URL, as the spec requires',
  /^Sitemap: https:\/\/cygenix\.co\.uk\/sitemap\.xml$/m.test(robots));
check('nothing in the sitemap is disallowed by robots — the two would contradict',
  (() => {
    const dis = (robots.match(/^Disallow: (\S+)$/gm) || []).map((l) => l.replace('Disallow: ', ''));
    const locs = [...read('sitemap.xml').matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => new URL(m[1]).pathname);
    return locs.every((p) => !dis.some((d) => p.startsWith(d)));
  })());

/* ── 2. sitemap.xml ─────────────────────────────────────────────────────── */

check('sitemap.xml is served from the publish root', fs.existsSync(path.join(PUB, 'sitemap.xml')));
const sitemap = read('sitemap.xml');
const locs = [...sitemap.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1]);
check('it declares the sitemap namespace and has entries',
  /xmlns="http:\/\/www\.sitemaps\.org\/schemas\/sitemap\/0\.9"/.test(sitemap) && locs.length >= 7);
check('every entry is an absolute https URL on the canonical host',
  locs.every((u) => /^https:\/\/cygenix\.co\.uk\//.test(u)), locs.join(' '));
// The site serves exactly one address per page and redirects the .html form
// away (scripts/build-routes.js). A sitemap listing .html would advertise the
// address the site itself 301s off.
check('and is the CLEAN address, never the .html one',
  locs.every((u) => !/\.html/.test(u)), locs.filter((u) => /\.html/.test(u)).join(' '));
const served = new Set(routes.pages().map((f) => (f === routes.LANDING ? '/' : routes.addressOf(f))));
const dead = locs.map((u) => new URL(u).pathname).filter((p) => !served.has(p));
check('every address in the sitemap resolves to a page that exists', dead.length === 0, dead.join(', '));
check('the homepage is listed at the root, at top priority',
  /<loc>https:\/\/cygenix\.co\.uk\/<\/loc>/.test(sitemap) && /<priority>1\.0<\/priority>/.test(sitemap));
// Stripped of comments first: the note at the top of the file explains when to
// change <lastmod>, and counting that mention as an entry would be wrong.
const sitemapBody = sitemap.replace(/<!--[\s\S]*?-->/g, '');
check('every entry carries a lastmod in ISO form',
  (sitemapBody.match(/<lastmod>/g) || []).length === locs.length
  && [...sitemapBody.matchAll(/<lastmod>([^<]+)<\/lastmod>/g)].every((m) => /^\d{4}-\d{2}-\d{2}$/.test(m[1])));

/* ── 3. llms.txt ────────────────────────────────────────────────────────── */
//
// The file assistants read verbatim. Everything in it is a claim made without
// a human in the loop to soften it, so the engine list has to match the
// manifest exactly: an engine that is `planned` must not appear at all.

check('llms.txt is served from the publish root', fs.existsSync(path.join(PUB, 'llms.txt')));
const llms = read('llms.txt');
check('it names the company and its registration, matching the footer',
  /Cygenix Ltd is a UK company \(registered in England and Wales, no\. 16063342\)/.test(llms)
  && /company number 16063342/.test(index));
check('its links are the same clean addresses the sitemap lists',
  [...llms.matchAll(/\]\((https:\/\/cygenix\.co\.uk[^)]*)\)/g)].map((m) => m[1])
    .every((u) => served.has(new URL(u).pathname)));
{
  // Against the manifest: every engine offered must be sellable, and every
  // sellable one should be there. `planned` engines must be absent entirely.
  const CAP = require(path.join(PUB, 'cygenix-capabilities.js'));
  const engines = [].concat(CAP.SOURCES, CAP.TARGETS);
  const sold = engines.filter((e) => !CAP.isSellable(e.status)).map((e) => e.label.split(' / ')[0])
    .filter((label) => new RegExp('\\b' + label + '\\b', 'i').test(llms));
  check('it offers no engine the manifest marks planned', sold.length === 0, [...new Set(sold)].join(', '));
  // llms.txt is prose, so it names an engine the way a person would rather
  // than repeating the manifest's label — "CSV/Excel/delimited files" for the
  // entry labelled "CSV, Excel and text". These patterns are what counts as
  // naming each one; a new engine in the manifest with no pattern here fails
  // rather than passing silently.
  const NAMED = {
    mssql: /\bSQL Server\b/i, azuresqldb: /\bAzure SQL Database\b/i,
    azuresqlmi: /\bAzure SQL Managed Instance\b/i, postgres: /\bPostgreSQL\b/i,
    files: /\bCSV\b[\s\S]*\bExcel\b/i, blob: /\bAzure Blob Storage\b/i,
  };
  const sellable = [...new Set(engines.filter((e) => CAP.isSellable(e.status)).map((e) => e.id))];
  const unpatterned = sellable.filter((id) => !NAMED[id]);
  check('every sellable engine has a pattern here, so a new one cannot slip through',
    unpatterned.length === 0, unpatterned.join(', '));
  const missing = sellable.filter((id) => NAMED[id] && !NAMED[id].test(llms));
  check('and llms.txt names every one of them', missing.length === 0, missing.join(', '));
  check('marking Managed Instance as beta, which is what the manifest says it is',
    /Azure SQL Managed Instance \(beta\)/.test(llms)
    && CAP.SOURCES.find((s) => s.id === 'azuresqlmi').status === 'beta');
}

/* ── 4. Canonicals ──────────────────────────────────────────────────────── */
//
// Without one, every URL variant that reaches a page — a trailing slash, a
// utm_ parameter someone pastes into LinkedIn — can be treated as a separate
// page and split its own ranking.

const CANON = { 'index.html': 'https://cygenix.co.uk/', 'pricing.html': 'https://cygenix.co.uk/pricing', 'about.html': 'https://cygenix.co.uk/about' };
Object.entries(CANON).forEach(([f, url]) => {
  const src = read(f);
  const m = /<link rel="canonical" href="([^"]+)">/.exec(src);
  check(f + ' declares its canonical, and it is the clean address', !!m && m[1] === url, m ? m[1] : 'none');
  check(f + ' agrees with the og:url it already advertised',
    !/og:url/.test(src) || new RegExp('og:url" content="' + url.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '"').test(src),
    (/<meta property="og:url" content="([^"]+)">/.exec(src) || [])[1]);
  check(f + "'s canonical is one of the addresses the sitemap lists", locs.includes(url));
});

/* ── 5. Meta descriptions fit the space they get ────────────────────────── */
//
// Google truncates around 155-160 characters. The homepage's ran to 191 and
// was cut mid-clause, so the last thing a searcher read was a fragment.

['index.html', 'pricing.html', 'about.html'].forEach((f) => {
  const m = /<meta name="description" content="([^"]*)">/.exec(read(f));
  check(f + ' has a description that will not be truncated (' + (m ? m[1].length : '?') + ' chars)',
    !!m && m[1].length > 50 && m[1].length <= 160, m ? m[1] : 'none');
});

/* ── 6. The structured data, against the facts it restates ──────────────── */

const blocks = (src) => [...src.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/g)]
  .map((m) => { try { return JSON.parse(m[1]); } catch (e) { return { parseError: e.message }; } });

const iLd = blocks(index), pLd = blocks(pricing), aLd = blocks(about);
const all = [].concat(iLd, pLd, aLd);
check('every JSON-LD block on every page is valid JSON',
  all.length === 4 && all.every((b) => !b.parseError), all.map((b) => b.parseError).filter(Boolean).join(' | '));
check('the homepage carries the Organization and the product; pricing the FAQ; about the consultancy',
  iLd.map((b) => b['@type']).join(',') === 'Organization,SoftwareApplication'
  && pLd.map((b) => b['@type']).join(',') === 'FAQPage'
  && aLd.map((b) => b['@type']).join(',') === 'ProfessionalService',
  [iLd, pLd, aLd].map((g) => g.map((b) => b['@type']).join(',')).join(' | '));

const org = iLd.find((b) => b['@type'] === 'Organization');
const app = iLd.find((b) => b['@type'] === 'SoftwareApplication');
const svc = aLd.find((b) => b['@type'] === 'ProfessionalService');
const faq = pLd.find((b) => b['@type'] === 'FAQPage');

// The footer is the legal statement of record. If the two ever disagree, one
// of them is telling a machine something untrue about a registered company.
check('the Organization\'s registered particulars are the footer\'s, exactly',
  org && org.identifier.value === '16063342'
  && org.address.streetAddress === '97 New Haw Road' && org.address.addressLocality === 'Addlestone'
  && org.address.addressRegion === 'Surrey' && org.address.postalCode === 'KT15 2DA'
  && index.includes('company number ' + org.identifier.value)
  && index.includes(org.address.streetAddress) && index.includes(org.address.addressLocality),
  org && JSON.stringify(org.address));
check('and its legal name is the one the footer uses', org && org.legalName === 'Cygenix Ltd' && /Cygenix Ltd/.test(index));

// TIER_PRICES on the pricing page is what a visitor is actually quoted.
{
  const t = /const TIER_PRICES = \{([\s\S]*?)\n\};/.exec(pricing);
  const gbp = t ? [...t[1].matchAll(/GBP:\s*\{\s*monthly:\s*(\d+)/g)].map((m) => Number(m[1])) : [];
  check('the advertised price range is the pricing page\'s own monthly GBP range',
    app && gbp.length >= 3 && Number(app.offers.lowPrice) === Math.min(...gbp)
    && Number(app.offers.highPrice) === Math.max(...gbp),
    app ? app.offers.lowPrice + '-' + app.offers.highPrice + ' vs ' + gbp.join('/') : 'no offer');
  check('and the tier count matches the cards on the page',
    app && Number(app.offers.offerCount) === (pricing.match(/<div class="tier[ "]/g) || []).length,
    app && app.offers.offerCount);
}

// A graph, not three unrelated descriptions.
check('the about page hangs off the homepage\'s Organization by @id',
  svc && svc.parentOrganization['@id'] === org['@id'] && org['@id'] === 'https://cygenix.co.uk/#organization');
check('and the product names the same publisher', app && app.publisher['@id'] === org['@id']);
check('the founder and the consultant are the same person, by @id',
  svc && org && svc.provider['@id'] === org.founder['@id']
  && svc.provider.name === org.founder.name && svc.provider.sameAs === org.founder.sameAs);
check('every url in the graph is an address the site serves',
  [org.url, app.url, svc.url].every((u) => served.has(new URL(u).pathname)), [org.url, app.url, svc.url].join(' '));

// The one way structured data can actively hurt: markup that says something
// the page does not. So the answers are re-derived from the visible accordion
// and compared, rather than trusted because they were right once.
{
  const dec = (s) => s.replace(/&amp;/g, '&').replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/\s+/g, ' ').trim();
  const visible = [...pricing.matchAll(/<button class="faq-q"[^>]*>\s*<span>([\s\S]*?)<\/span>[\s\S]*?<div class="faq-a">\s*<p>([\s\S]*?)<\/p>/g)]
    .map((m) => ({ q: dec(m[1]), a: dec(m[2]) }));
  check('the FAQ markup covers every question the page actually shows',
    faq && visible.length >= 10 && faq.mainEntity.length === visible.length,
    faq ? faq.mainEntity.length + ' marked up vs ' + visible.length + ' visible' : 'no FAQPage');
  const drift = (faq ? faq.mainEntity : []).map((e, i) => {
    const v = visible[i];
    if (!v) return 'extra: ' + e.name;
    if (e.name !== v.q) return 'question ' + (i + 1) + ': "' + e.name + '" vs visible "' + v.q + '"';
    if (e.acceptedAnswer.text !== v.a) return 'answer ' + (i + 1) + ' differs from the visible text';
    return null;
  }).filter(Boolean);
  check('and every question and answer matches the visible text exactly', drift.length === 0, drift.join(' | '));
  check('no answer is still a placeholder', !/PASTE YOUR/i.test(pricing));
}

console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
process.exit(fail ? 1 : 0);
