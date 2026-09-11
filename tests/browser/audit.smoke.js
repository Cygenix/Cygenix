/* tests/browser/audit.smoke.js
 * ---------------------------------------------------------------------------
 * The Audit Log screen, in a browser, against a stub of its own API.
 *
 * The server side of this feature is covered from Node by audit-api.test.js,
 * which drives the real handler. What that cannot tell you is whether the
 * screen renders, whether the tabs are reachable from a keyboard, whether the
 * drawer opens on the right row, whether a pause modal actually refuses an
 * empty reason, and whether any of it throws. Those are the failures that
 * reach a user, and they only happen in a browser.
 *
 * So the audit function is stubbed at the network — every shape it can return
 * is scripted here — and the real audit-app.js is put through it.
 *
 * The one thing deliberately NOT stubbed is the decision logic: this file
 * never asserts what should be recorded, only what the screen does with the
 * answer. Two tests asserting the same rule in two places is how a rule ends
 * up changed in one of them.
 *
 * Not part of `npm test`: it needs a browser.
 *   node tests/browser/audit.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8412);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json' };

const ROUTES = (() => {
  const map = {};
  fs.readFileSync(path.join(PUB, '_redirects'), 'utf8').split('\n').forEach((line) => {
    const m = line.trim().match(/^(\/\S*)\s+(\/\S+)\s+200$/);
    if (m) map[m[1]] = m[2];
  });
  return map;
})();

const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/') p = '/index.html';
  if (p === '/auth-gate.js') {
    res.writeHead(200, { 'Content-Type': TYPES['.js'] });
    return res.end('/* stubbed: the audit screen is under test, not the auth gate */');
  }
  if (ROUTES[p]) p = ROUTES[p];
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) {
    res.writeHead(404); return res.end('no');
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

const SEED = () => {
  localStorage.setItem('cygenix_user', JSON.stringify({ email: 'owner@example.test', name: 'An Owner' }));
  localStorage.setItem('cygenix_active_user', 'owner@example.test');
  localStorage.setItem('cygenix_cookie_consent', 'all');
  localStorage.setItem('cygenix_token', 'smoke-token');
  localStorage.setItem('cygenix_projects', JSON.stringify([{ id: 'p1', name: 'Demo migration' }]));
  localStorage.setItem('cygenix_active_project_id', 'p1');
  // The first-run modal appears 800ms in and covers the whole screen. It is
  // not what is under test and it intercepts every click.
  localStorage.setItem('cygenix_onboarded', '1');
};

const CATEGORIES = [
  { key: 'security', label: 'Security', alwaysOn: true, detail: 'Sign-in, MFA, API keys.' },
  { key: 'access', label: 'Access & roles', alwaysOn: true, detail: 'Users, invitations, roles.' },
  { key: 'prod', label: 'Production changes', alwaysOn: true, detail: 'Writes against PROD.' },
  { key: 'audit', label: 'The log itself', alwaysOn: true, detail: 'State, settings, exports.' },
  { key: 'settings', label: 'Settings', alwaysOn: false, detail: 'Project and system settings.' },
  { key: 'jobs', label: 'Jobs', alwaysOn: false, detail: 'Create, run, reorder.' },
];

// Hosts dashboard.html itself references. Derived, not listed.
const PAGE_HOSTS = (() => {
  const html = fs.readFileSync(path.join(PUB, 'dashboard.html'), 'utf8');
  const hosts = new Set(['fonts.gstatic.com']);   // pulled in BY the stylesheet
  for (const m of html.matchAll(/https:\/\/([a-z0-9.-]+)\//gi)) hosts.add(m[1]);
  return [...hosts];
})();

const iso = (minsAgo) => new Date(Date.now() - minsAgo * 60000).toISOString();

const EVENTS = [
  { seq: 9, id: 'AAAAAAAAAAAAAAAAAAAAAAAAAA', occurredAt: iso(2), actorEmail: 'owner@example.test',
    actorName: 'An Owner', effectiveRoles: ['OW', 'PA'], action: 'sysparam.update', category: 'settings',
    resourceType: 'system_parameter', resourceId: 'batch_size',
    target: { type: 'system_parameter', id: 'batch_size', label: 'System Parameters > Batch size' },
    environment: 'DEV', outcome: 'allowed', severity: 'info', actorType: 'user', source: 'server',
    summary: 'Changed batch size 5,000 to 10,000',
    changes: [{ field: 'batchSize', before: 5000, after: 10000 },
              { field: 'apiKey', before: '(redacted)', after: '(redacted)' }],
    context: { ip: '203.0.113.7', userAgent: 'Chrome' },
    prevHash: 'p'.repeat(64), entryHash: 'h'.repeat(64) },
  { seq: 8, id: 'BBBBBBBBBBBBBBBBBBBBBBBBBB', occurredAt: iso(40), actorEmail: 'eng@example.test',
    actorName: 'An Engineer', effectiveRoles: ['EN'], action: 'sql.write', category: 'prod',
    environment: 'PROD', outcome: 'denied', severity: 'high', actorType: 'user', source: 'server',
    summary: 'Refused a write against CRM_PROD',
    detail: { reason: 'Production execution requires the Migration Lead role' },
    target: { type: 'connection', id: 'crm', label: 'Target: CRM_PROD' },
    context: { ip: '203.0.113.9' }, prevHash: 'q'.repeat(64), entryHash: 'p'.repeat(64) },
  { seq: 7, id: 'CCCCCCCCCCCCCCCCCCCCCCCCCC', occurredAt: iso(90), actorEmail: 'lead@example.test',
    actorName: 'A Lead', effectiveRoles: ['ML'], action: 'mapping.ai-apply', category: 'jobs',
    environment: 'DEV', outcome: 'allowed', severity: 'info', actorType: 'assistant',
    onBehalfOf: 'lead@example.test', source: 'client',
    summary: 'Ask Cygenix mapped 14 columns on Orders',
    target: { type: 'mapping', id: 'orders', label: 'Mapping: Orders' },
    context: { ip: '203.0.113.11' }, prevHash: 'r'.repeat(64), entryHash: 'q'.repeat(64) },
];

// Server behaviour the screen has to cope with, switched per test.
const world = {
  scope: 'organisation',
  canConfigure: true,
  canExport: true,
  canVerify: true,
  state: 'recording',
  denyAll: false,
  settings: {
    categories: { security: true, access: true, prod: true, audit: true, settings: true, jobs: true },
    retentionDays: 365, pauseMaxMinutes: 240,
    storeDiffs: true, storeIp: true, recordAssistant: true,
    archiveBeforePurge: true,
  },
  checkpoint: { purged: false, chainStartsAt: 1 },
  // Deliberately inside the window the three events span. A gap older than
  // the oldest loaded row is correctly NOT drawn — it belongs below the page,
  // and drawing it at the bottom would put it in the wrong place in time.
  gaps: [{ kind: 'paused', from: iso(80), to: iso(50), open: false,
           by: 'owner@example.test', reason: 'bulk re-import, too noisy', endedBy: 'system' }],
  posts: [],
  lastEventsUrl: '',
};

function statusBody() {
  return {
    state: world.state, storedState: world.state,
    pausedUntil: world.state === 'paused' ? iso(-120) : null,
    reason: world.state === 'recording' ? null : 'bulk re-import, too noisy',
    changedBy: world.state === 'recording' ? null : 'owner@example.test',
    changedAt: iso(5),
    settings: world.settings,
    checkpoint: world.checkpoint,
    categories: CATEGORIES,
    alwaysOn: ['security', 'access', 'prod', 'audit'],
    pausePresets: [30, 60, 120, 240],
    pauseMaxOptions: [240, 1440],
    retentionOptions: [90, 365, 2555],
    stats: world.scope === 'self' ? null
      : { events: 42, actors: 3, prodChanges: 2, deniedOrFailed: 1, windowMs: 86400000, indexed: true },
    gaps: world.gaps,
    scope: world.scope,
    canConfigure: world.canConfigure,
    canExport: world.canExport,
    canVerify: world.canVerify,
    me: 'owner@example.test',
  };
}

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 960 } });

  const offSite = [];
  await ctx.route('**', (route) => {
    const u = route.request().url();
    if (u.indexOf('/.netlify/functions/audit') !== -1) {
      const json = (status, body) => route.fulfill({
        status, contentType: 'application/json', body: JSON.stringify(body) });

      if (world.denyAll) return json(403, { error: 'Not permitted: no grant for audit.read' });

      if (route.request().method() === 'POST') {
        const sent = JSON.parse(route.request().postData() || '{}');
        world.posts.push(sent);
        if (sent.op === 'status') {
          world.state = sent.state;
          return json(200, { done: true, state: sent.state, pausedUntil: statusBody().pausedUntil,
                             notify: sent.state === 'off'
                               ? { adminsEmailed: false, reason: 'no server-held SMTP credentials' } : null,
                             settings: world.settings });
        }
        if (sent.op === 'purge') {
          world.checkpoint = {
            purged: true, chainStartsAt: 5, purgedTotal: 4, archived: true,
            anchorHash: 'a'.repeat(64), anchorAt: iso(600),
            lastPurgeAt: iso(0), archiveKey: 'audit/archive/0000000001-0000000004',
          };
          return json(200, { done: true, purged: 4, deleted: 4, archived: true,
                             archiveKey: world.checkpoint.archiveKey, retentionDays: 365,
                             more: false, checkpoint: world.checkpoint });
        }
        if (sent.op === 'settings') {
          if (sent.categories && sent.categories.prod === false) {
            return json(400, { error: 'the prod category cannot be disabled' });
          }
          Object.assign(world.settings, sent);
          if (sent.categories) Object.assign(world.settings.categories, sent.categories);
          return json(200, { done: true, settings: world.settings, changes: [{ field: 'x' }] });
        }
        return json(200, { recorded: true, id: 'X'.repeat(26), seq: 10 });
      }

      if (u.indexOf('what=verify') !== -1) {
        return json(200, { ok: true, count: 9, verifiedAt: new Date().toISOString(), tookMs: 12 });
      }
      if (u.indexOf('what=export') !== -1) {
        return route.fulfill({ status: 200, contentType: 'text/csv',
          headers: { 'Content-Disposition': 'attachment; filename="a.csv"' },
          body: '﻿seq,action\r\n9,sysparam.update\r\n' });
      }
      if (u.indexOf('what=events') !== -1) {
        let rows = EVENTS.slice();
        const param = (k) => {
          const m = new RegExp('[?&]' + k + '=([^&]*)').exec(u);
          return m && m[1] ? decodeURIComponent(m[1]) : '';
        };
        if (param('category')) rows = rows.filter((e) => e.category === param('category'));
        if (param('outcome')) rows = rows.filter((e) => e.outcome === param('outcome'));
        if (param('env')) rows = rows.filter((e) => e.environment === param('env'));
        if (param('actor')) rows = rows.filter((e) => e.actorEmail === param('actor'));
        if (param('action')) rows = rows.filter((e) => e.action === param('action'));
        if (param('target')) {
          const t = param('target').toLowerCase();
          rows = rows.filter((e) => ((e.target && e.target.label) || '').toLowerCase().indexOf(t) !== -1);
        }
        if (param('q')) {
          const t = param('q').toLowerCase();
          rows = rows.filter((e) => (e.action + ' ' + (e.summary || '')).toLowerCase().indexOf(t) !== -1);
        }
        world.lastEventsUrl = u;
        return json(200, {
          total: rows.length, chainTotal: 9, entries: rows,
          indexed: true, nextCursor: null, scope: world.scope,
          // Facets are computed over the whole window, NOT over the filtered
          // rows — the server does the same, so that picking a person does
          // not empty the action list of everything they did not do.
          facets: {
            actors: [...new Set(EVENTS.map((e) => e.actorEmail))]
              .map((v) => ({ value: v, count: 1 })),
            actions: [...new Set(EVENTS.map((e) => e.action))]
              .map((v) => ({ value: v, count: 1 })),
          },
        });
      }
      return json(200, statusBody());
    }
    if (u.startsWith('http://localhost:' + PORT)) return route.continue();
    // dashboard.html reaches for webfonts, MSAL and a handful of parsing
    // libraries from CDNs of its own accord. Those are the page's, not this
    // screen's. The allowed set is read out of the page's own markup rather
    // than listed here, so this stays honest: a host the audit log invented
    // is reported, and a host dashboard.html adds tomorrow does not fail an
    // unrelated test.
    try {
      if (PAGE_HOSTS.indexOf(new URL(u).host) === -1) offSite.push(u);
    } catch (e) { offSite.push(u); }
    return route.abort();
  });

  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('console', (m) => {
    if (m.type() === 'error' && !/ERR_|Failed to load resource/.test(m.text())) errors.push(m.text());
  });
  await page.addInitScript(SEED);

  const openAudit = async () => {
    await page.waitForFunction(() => !!window.CygenixAuditView, null, { timeout: 20000 });
    await page.evaluate(() => {
      // audit-app.js takes its bearer token from getCygenixIdToken(), whose
      // real implementation digs an IdToken out of MSAL's localStorage cache.
      // Standing up a valid MSAL cache would be testing cygenix-auth-token.js,
      // not this screen — so it is replaced here, AFTER that script has
      // loaded and defined its own (an init script would be overwritten).
      window.getCygenixIdToken = () => 'smoke-token';
      document.querySelectorAll('.view').forEach((v) => v.classList.remove('active'));
      const v = document.getElementById('view-audit');
      v.classList.add('active');
      v.style.display = 'block';
      window.CygenixAuditView.render(document.getElementById('audit-log-wrap'));
    });
    await page.waitForSelector('.cyg-a-status', { timeout: 10000 });
  };

  console.log('Audit Log — the screen, in a browser\n');

  // ── 1. It renders ───────────────────────────────────────────────────────
  console.log('1. The screen');
  await page.goto('http://localhost:' + PORT + '/dashboard', { waitUntil: 'domcontentloaded' });
  await openAudit();

  check('the status card renders', await page.isVisible('.cyg-a-status'));
  check('and says Recording', (await page.textContent('.cyg-a-status')).indexOf('Recording') !== -1);
  check('the always-on sentence is on the card, not buried in settings',
    (await page.textContent('.cyg-a-always')).indexOf('always recorded') !== -1);
  check('four KPI tiles render', (await page.$$('.cyg-a-kpi')).length === 4);
  check('the KPI values come from the server, not from counting rows',
    (await page.textContent('.cyg-a-kpis')).indexOf('42') !== -1);
  check('three events are listed', (await page.$$('#cyg-a-rows tr.ev')).length === 3);
  check('a denied row is drawn as denied',
    (await page.textContent('#cyg-a-rows')).indexOf('denied') !== -1);
  check('a PROD row is marked PROD', (await page.$$('#cyg-a-rows .cyg-a-env.PROD')).length === 1);
  check('an assistant action is tagged "via Ask Cygenix"',
    (await page.textContent('#cyg-a-rows')).indexOf('via Ask Cygenix') !== -1);
  check('a client-recorded event is distinguishable from a server-observed one',
    (await page.$$('#cyg-a-rows .cyg-a-src')).length === 1);

  // The gap row is the point of showing pause windows at all.
  check('a past pause window is drawn as a gap row', (await page.$$('#cyg-a-rows tr.gap')).length === 1);
  check('the gap names who paused and why',
    (await page.textContent('#cyg-a-rows tr.gap')).indexOf('too noisy') !== -1);
  check('and says the always-on categories were still recorded through it',
    /still recorded/.test(await page.textContent('#cyg-a-rows tr.gap')));

  check('the legacy browser-local table is gone',
    (await page.textContent('#audit-log-wrap')).indexOf('No browser-local entries') === -1);
  check('and so is the "full trail on Users & Roles" footer',
    (await page.textContent('#audit-log-wrap')).indexOf('Users &amp; Roles') === -1 &&
    (await page.textContent('#audit-log-wrap')).indexOf('full trail, verification and export') === -1);

  // ── 2. Tabs ─────────────────────────────────────────────────────────────
  console.log('\n2. Tabs, from a keyboard');
  check('the tablist is a real tablist', (await page.$$('[role="tab"]')).length === 3);
  check('only the selected tab is in the tab order',
    (await page.$$('[role="tab"][tabindex="0"]')).length === 1);
  await page.focus('#cyg-a-tab-events');
  await page.keyboard.press('ArrowRight');
  check('arrow keys move between tabs',
    await page.getAttribute('#cyg-a-tab-settings', 'aria-selected') === 'true');
  check('and the matching panel is the only one shown',
    !(await page.getAttribute('#cyg-a-panel-settings', 'hidden')) &&
    (await page.getAttribute('#cyg-a-panel-events', 'hidden')) !== null);
  await page.keyboard.press('End');
  check('End jumps to the last tab',
    await page.getAttribute('#cyg-a-tab-integrity', 'aria-selected') === 'true');

  // ── 3. Integrity ────────────────────────────────────────────────────────
  console.log('\n3. Integrity');
  check('the last chain blocks are drawn', (await page.$$('.cyg-a-blk')).length === 3);
  check('the known limit is stated on the page, not left to the docs',
    /tamper-evident, not tamper-proof/.test(await page.textContent('#cyg-a-panel-integrity')));
  await page.click('#cyg-a-verify');
  await page.waitForFunction(() => /intact/.test(
    document.getElementById('cyg-a-verifyout').textContent), null, { timeout: 8000 });
  check('verifying reports the result', /9 entries/.test(await page.textContent('#cyg-a-verifyout')));

  // Retention. A chain that has never been purged must say so, rather than
  // repeating the policy back at the reader as though it had happened.
  const integrity = () => page.textContent('#cyg-a-panel-integrity');
  check('the Integrity tab has a retention panel', await page.isVisible('#cyg-a-purge'));
  check('and says nothing has been purged yet',
    /Nothing has been purged yet/.test(await integrity()));
  check('explaining why deleting will not break verification',
    /checkpoint/.test(await integrity()));
  check('and claiming no signing key, because there is none',
    /no signing key involved, and none\s+is claimed/.test((await integrity()).replace(/\s+/g, ' ')) ||
    /none is claimed/.test(await integrity()));

  page.once('dialog', (d) => d.accept());
  await page.click('#cyg-a-purge');
  await page.waitForFunction(() => /Purged 4/.test(
    document.getElementById('cyg-a-purgeout') ? document.getElementById('cyg-a-purgeout').textContent : ''),
    null, { timeout: 8000 });
  check('running retention reports what it purged',
    /Purged 4 entries \(archived\)/.test(await page.textContent('#cyg-a-purgeout')));
  check('and the panel now says where the chain begins',
    /begins at entry <?#?5|begins at entry/.test(await integrity()) &&
    /#5/.test(await integrity()));
  check('showing the checkpoint hash in full, not a friendly prefix',
    (await integrity()).indexOf('a'.repeat(64)) !== -1);
  check('and naming the archive it went to',
    /audit\/archive\/0000000001-0000000004/.test(await integrity()));

  // Erasure has to read differently from archiving, because it is different.
  world.settings.archiveBeforePurge = false;
  await openAudit();
  await page.click('#cyg-a-tab-settings');
  check('with archiving off the settings panel warns that entries are erased',
    /permanently erased/i.test(await page.textContent('#cyg-a-panel-settings')));
  check('and says it cannot be undone',
    /cannot be undone/i.test(await page.textContent('#cyg-a-panel-settings')));
  world.settings.archiveBeforePurge = true;
  world.checkpoint = { purged: false, chainStartsAt: 1 };
  await openAudit();
  await page.click('#cyg-a-tab-integrity');

  // ── 4. Settings ─────────────────────────────────────────────────────────
  console.log('\n4. Capture settings');
  await page.click('#cyg-a-tab-settings');
  const locked = await page.$$eval('#cyg-a-cats input[type=checkbox]',
    (els) => els.filter((e) => e.disabled).length);
  check('the four always-on categories are locked in the UI', locked === 4);
  check('and an optional one is not', await page.isEnabled('#cyg-a-cats input[data-cat-key="settings"]'));

  await page.uncheck('#cyg-a-cats input[data-cat-key="jobs"]');
  await page.waitForFunction(() => document.getElementById('cyg-a-toast'), null, { timeout: 5000 });
  const catPost = world.posts.filter((p) => p.op === 'settings').pop();
  check('turning a category off posts it to the server',
    !!catPost && catPost.categories && catPost.categories.jobs === false);

  await page.click('#cyg-a-panel-settings .cyg-a-radio input[value="90"]');
  const retPost = world.posts.filter((p) => p.op === 'settings' && p.retentionDays).pop();
  check('changing retention posts it', !!retPost && retPost.retentionDays === 90);

  // This used to assert the opposite — that the purge job did not exist — and
  // the assertion changing is the point: the copy has to move when the
  // behaviour does, or the screen keeps telling a reader something that
  // stopped being true.
  check('the page says retention runs nightly',
    /Runs nightly/.test(await page.textContent('#cyg-a-panel-settings')));
  check('and no longer claims the job is unbuilt',
    !/not running yet/.test(await page.textContent('#cyg-a-panel-settings')));
  check('the archive-or-erase choice is offered',
    await page.isVisible('#cyg-a-panel-settings [data-flag="archiveBeforePurge"]'));
  check('and that admins are notified in-app because there is no server mail',
    /no server-held mail credentials/.test(await page.textContent('#cyg-a-panel-settings')));

  // ── 5. The drawer ───────────────────────────────────────────────────────
  console.log('\n5. The drawer');
  await page.click('#cyg-a-tab-events');
  await page.click('#cyg-a-rows tr.ev[data-seq="9"]');
  await page.waitForSelector('#cyg-a-drawer.on', { timeout: 5000 });
  const drawer = await page.textContent('#cyg-a-dbody');
  check('the drawer opens on the row that was clicked',
    (await page.textContent('#cyg-a-dact')) === 'sysparam.update');
  check('it shows the before/after diff', (await page.$$('.cyg-a-diff .row')).length === 3);
  check('a redacted field is shown as changed but not as a value',
    drawer.indexOf('(redacted)') !== -1);
  check('it shows the full hash, not a friendly prefix',
    drawer.indexOf('h'.repeat(64)) !== -1);
  check('and the previous hash it links to', drawer.indexOf('p'.repeat(64)) !== -1);
  check('it carries the machine-readable timestamp as well as the friendly one',
    /\d{4}-\d{2}-\d{2}T/.test(drawer));
  check('the row is marked as selected', (await page.$$('#cyg-a-rows tr.ev.sel')).length === 1);

  await page.keyboard.press('Escape');
  await page.waitForFunction(() => !document.querySelector('#cyg-a-drawer.on'), null, { timeout: 4000 });
  check('Escape closes it', (await page.$$('#cyg-a-drawer.on')).length === 0);
  check('and focus comes back to the row rather than the top of the document',
    await page.evaluate(() => document.activeElement &&
      document.activeElement.matches('#cyg-a-rows tr.ev')));

  // Keyboard route into the drawer.
  await page.focus('#cyg-a-rows tr.ev[data-seq="8"]');
  await page.keyboard.press('Enter');
  await page.waitForSelector('#cyg-a-drawer.on', { timeout: 4000 });
  check('Enter on a focused row opens it too',
    (await page.textContent('#cyg-a-dact')) === 'sql.write');
  check('a denial shows the reason it was refused',
    /Migration Lead role/.test(await page.textContent('#cyg-a-dbody')));
  await page.keyboard.press('Escape');

  // ── 6. Filters, in the column headers ───────────────────────────────────
  console.log('\n6. Filters');

  check('there is a filter control in every column header',
    (await page.$$('.cyg-a-filters [data-filter]')).length === 6);
  check('and each is labelled for a screen reader',
    await page.$$eval('.cyg-a-filters [data-filter]',
      (els) => els.every((e) => !!e.getAttribute('aria-label'))));
  check('the filter row is sticky, so you can still see which column it governs',
    await page.$eval('.cyg-a-filters th',
      (e) => getComputedStyle(e).position === 'sticky'));

  await page.selectOption('[data-filter="env"]', 'PROD');
  await page.waitForFunction(() => document.querySelectorAll('#cyg-a-rows tr.ev').length === 1,
    null, { timeout: 6000 });
  check('the ENV header filters the list', (await page.$$('#cyg-a-rows tr.ev')).length === 1);
  check('a set filter is visibly set, so an empty table is not mistaken for an empty log',
    await page.$eval('[data-filter="env"]', (e) => e.classList.contains('on')));
  check('and a Clear control appears', await page.isVisible('#cyg-a-clear'));

  await page.selectOption('[data-filter="actor"]', 'lead@example.test');
  await page.waitForFunction(() => document.querySelectorAll('#cyg-a-rows tr.ev').length === 0,
    null, { timeout: 6000 });
  check('two header filters combine rather than replacing each other',
    /env=PROD/.test(world.lastEventsUrl) && /actor=lead/.test(world.lastEventsUrl));
  check('and the empty result says so rather than looking broken',
    /No events match/.test(await page.textContent('#cyg-a-rows')));

  await page.click('#cyg-a-clear');
  await page.waitForFunction(() => document.querySelectorAll('#cyg-a-rows tr.ev').length === 3,
    null, { timeout: 6000 });
  check('Clear resets every filter at once', (await page.$$('#cyg-a-rows tr.ev')).length === 3);
  check('and the Clear control goes away with them',
    (await page.$$('#cyg-a-clear')).length === 0);

  // The person and action lists come from the server's facets, over the whole
  // window — a dropdown built from the fifty rows on screen silently omits
  // the person somebody is looking for.
  check('the person list is built from the whole window, not the page',
    (await page.$$('[data-filter="actor"] option')).length === 4);
  check('the action list too', (await page.$$('[data-filter="action"] option')).length === 4);
  check('and each option says how many events it covers',
    /\(\d+\)/.test(await page.textContent('[data-filter="actor"]')));

  await page.selectOption('[data-filter="action"]', 'sql.write');
  await page.waitForFunction(() => document.querySelectorAll('#cyg-a-rows tr.ev').length === 1,
    null, { timeout: 6000 });
  check('the ACTION header filter narrows to one action',
    (await page.textContent('#cyg-a-rows')).indexOf('sql.write') !== -1);
  check('picking a person does not empty the action list of everything else',
    (await page.$$('[data-filter="action"] option')).length === 4);
  await page.click('#cyg-a-clear');
  await page.waitForFunction(() => document.querySelectorAll('#cyg-a-rows tr.ev').length === 3,
    null, { timeout: 6000 });

  // TARGET had no filter at all before, which on a table this wide is the
  // column you most want one on.
  await page.fill('[data-filter="target"]', 'CRM');
  await page.waitForFunction(() => document.querySelectorAll('#cyg-a-rows tr.ev').length === 1,
    null, { timeout: 6000 });
  check('the TARGET header filter narrows on the target',
    (await page.textContent('#cyg-a-rows')).indexOf('CRM_PROD') !== -1);
  check('and it is sent as its own parameter, not folded into the free-text search',
    /target=CRM/i.test(world.lastEventsUrl) && !/[?&]q=CRM/i.test(world.lastEventsUrl));

  // Typing into a debounced box that rebuilds itself is where focus goes to
  // die. page.fill() sets a value in one go and would never notice.
  await page.click('#cyg-a-clear');
  await page.waitForFunction(() => document.querySelectorAll('#cyg-a-rows tr.ev').length === 3,
    null, { timeout: 6000 });
  await page.focus('[data-filter="target"]');
  await page.keyboard.type('CR', { delay: 30 });
  await page.waitForTimeout(700);          // past the debounce and the re-render
  await page.keyboard.type('M', { delay: 30 });
  await page.waitForTimeout(700);
  check('the caret survives the re-render a debounced filter causes',
    await page.$eval('[data-filter="target"]',
      (e) => document.activeElement === e && e.value === 'CRM'),
    await page.$eval('[data-filter="target"]', (e) => e.value));
  check('and the third keystroke landed in the box, not nowhere',
    (await page.$$('#cyg-a-rows tr.ev')).length === 1);

  await page.click('#cyg-a-clear');
  await page.waitForFunction(() => document.querySelectorAll('#cyg-a-rows tr.ev').length === 3,
    null, { timeout: 6000 });

  // Free text still spans every column — the header filters narrow one
  // column each, and neither replaces the other.
  await page.focus('#cyg-a-q');
  await page.keyboard.type('batch', { delay: 20 });
  await page.waitForFunction(() => document.querySelectorAll('#cyg-a-rows tr.ev').length === 1,
    null, { timeout: 6000 });
  check('the free-text search still spans everything', (await page.$$('#cyg-a-rows tr.ev')).length === 1);
  check('and typing into it keeps focus too',
    await page.$eval('#cyg-a-q', (e) => document.activeElement === e && e.value === 'batch'));
  await page.click('#cyg-a-clear');
  await page.waitForFunction(() => document.querySelectorAll('#cyg-a-rows tr.ev').length === 3,
    null, { timeout: 6000 });

  await page.click('#cyg-a-chips [data-cat="prod"]');
  await page.waitForFunction(() => document.querySelectorAll('#cyg-a-rows tr.ev').length === 1,
    null, { timeout: 6000 });
  check('a category chip still filters the list', (await page.$$('#cyg-a-rows tr.ev')).length === 1);
  check('and the chip reports itself pressed',
    await page.getAttribute('#cyg-a-chips [data-cat="prod"]', 'aria-pressed') === 'true');
  check('gap rows are dropped while a filter is on — a gap row inside a ' +
        'filtered list would be claiming something it is not saying',
    (await page.$$('#cyg-a-rows tr.gap')).length === 0);
  await page.click('#cyg-a-clear');
  await page.waitForFunction(() => document.querySelectorAll('#cyg-a-rows tr.ev').length === 3,
    null, { timeout: 6000 });

  // ── 7. Pause and Off ────────────────────────────────────────────────────
  console.log('\n7. Pause and Off');
  await page.click('#cyg-a-state-seg button[data-s="paused"]');
  await page.waitForSelector('#cyg-a-modal.on', { timeout: 4000 });
  check('pausing asks first', await page.isVisible('#cyg-a-why'));
  const before = world.posts.length;
  await page.click('#cyg-a-mbox [data-m="pause"]');
  await page.waitForTimeout(200);
  check('an empty reason is refused in the browser, before the round trip',
    world.posts.length === before);
  check('and the field is marked',
    (await page.getAttribute('#cyg-a-why', 'style') || '').indexOf('var(--red)') !== -1);

  await page.fill('#cyg-a-why', 'bulk re-import of 40 staging tables');
  await page.selectOption('#cyg-a-dur', '120');
  await page.click('#cyg-a-mbox [data-m="pause"]');
  await page.waitForFunction(() => {
    const c = document.querySelector('.cyg-a-status');
    return !!c && c.className.split(/\s+/).indexOf('paused') !== -1;
  }, null, { timeout: 8000 });
  const pausePost = world.posts.filter((p) => p.op === 'status' && p.state === 'paused').pop();
  check('a pause posts the reason and the duration',
    !!pausePost && pausePost.pauseMinutes === 120 && /staging tables/.test(pausePost.reason));
  check('the card switches to the paused treatment',
    (await page.getAttribute('.cyg-a-status', 'class')).indexOf('paused') !== -1);
  check('and says when it resumes by itself',
    /Resumes by itself/.test(await page.textContent('.cyg-a-status')));

  await page.click('#cyg-a-state-seg button[data-s="off"]');
  await page.waitForSelector('#cyg-a-modal.on', { timeout: 4000 });
  await page.fill('#cyg-a-why', 'decommissioning the environment');
  const beforeOff = world.posts.length;
  await page.click('#cyg-a-mbox [data-m="off"]');
  await page.waitForTimeout(200);
  check('turning off without typing OFF is refused in the browser too',
    world.posts.length === beforeOff);
  await page.fill('#cyg-a-conf', 'off');
  await page.click('#cyg-a-mbox [data-m="off"]');
  // Wait on the CLASS, not on the word "Off" appearing anywhere in the card:
  // the segmented control has a button labelled Off, so a text match passes
  // before the state has actually changed and the next assertion races it.
  await page.waitForFunction(() => {
    const c = document.querySelector('.cyg-a-status');
    return !!c && c.className.split(/\s+/).indexOf('off') !== -1;
  }, null, { timeout: 8000 });
  check('lower-case off is accepted — the check is on the word, not the shift key',
    !!world.posts.filter((p) => p.op === 'status' && p.state === 'off').pop());
  check('the card switches to the off treatment',
    (await page.getAttribute('.cyg-a-status', 'class')).indexOf('off') !== -1);
  check('and says off has no timer',
    /no timer/.test(await page.textContent('.cyg-a-status')));

  await page.click('#cyg-a-state-seg button[data-s="recording"]');
  await page.waitForFunction(() => /Recording/.test(
    document.querySelector('.cyg-a-status').textContent), null, { timeout: 8000 });
  check('resuming needs no modal — restoring the record is not the risky direction',
    (await page.$$('#cyg-a-modal.on')).length === 0);

  // ── 8. Export ───────────────────────────────────────────────────────────
  console.log('\n8. Export');
  check('the CSV export is the primary action on the toolbar',
    await page.$eval('#cyg-a-csv', (e) => e.classList.contains('primary')));
  check('and says what it does rather than showing a bare arrow',
    /Export CSV/.test(await page.textContent('#cyg-a-csv')));
  const dl = page.waitForEvent('download', { timeout: 8000 }).catch(() => null);
  await page.click('#cyg-a-csv');
  const download = await dl;
  check('CSV export produces a download', !!download);
  check('named for the audit log and the day',
    !!download && /^cygenix_audit_\d{4}-\d{2}-\d{2}\.csv$/.test(download.suggestedFilename()));

  // ── 9. A reader who may not configure ───────────────────────────────────
  console.log('\n9. An Auditor: reads everything, changes nothing');
  world.canConfigure = false;
  world.canExport = true;
  await openAudit();
  check('the state control is disabled rather than hidden — the state is a fact worth seeing',
    (await page.$$eval('#cyg-a-state-seg button', (els) => els.every((e) => e.disabled))));
  await page.click('#cyg-a-tab-settings');
  check('every settings control is disabled',
    await page.$$eval('#cyg-a-panel-settings input', (els) => els.every((e) => e.disabled)));
  check('and the page says why, naming the separation rather than just refusing',
    /an auditor who\s+can quieten the trail they report on is not an auditor/
      .test((await page.textContent('#cyg-a-panel-settings')).replace(/\s+/g, ' ')) ||
    /not an auditor/.test(await page.textContent('#cyg-a-panel-settings')));

  // ── 10. A self-scoped reader ────────────────────────────────────────────
  console.log('\n10. A delivery role: their own entries');
  world.scope = 'self';
  await openAudit();
  check('no organisation KPI tiles are drawn for a self-scoped reader',
    (await page.$$('.cyg-a-kpi')).length === 0);
  check('and the page says whose entries these are',
    /your own entries/.test(await page.textContent('#audit-log-wrap')));

  // ── 11. Refused outright ────────────────────────────────────────────────
  console.log('\n11. Refused');
  world.denyAll = true;
  await openAudit().catch(() => {});
  await page.waitForSelector('.cyg-a-denied', { timeout: 8000 });
  check('a 403 renders the refusal panel rather than an error', await page.isVisible('.cyg-a-denied'));
  check('which names the roles that can read the trail',
    /Auditor/.test(await page.textContent('.cyg-a-denied')));
  check('and tells the reader the attempt was itself recorded',
    /audit\.view\.denied/.test(await page.textContent('.cyg-a-denied')));

  // ── 12. Nothing broke ───────────────────────────────────────────────────
  console.log('\n12. Hygiene');
  check('no uncaught errors on any of that', errors.length === 0, errors.slice(0, 3).join(' | '));
  check('nothing reached off-site', offSite.length === 0, offSite.slice(0, 3).join(' | '));

  const overflow = await page.evaluate(() => document.documentElement.scrollWidth
    - document.documentElement.clientWidth);
  check('no horizontal overflow at 1440', overflow <= 0, 'overflow ' + overflow);

  world.denyAll = false;
  world.scope = 'organisation';
  world.canConfigure = true;
  for (const w of [1024, 768, 390]) {
    await page.setViewportSize({ width: w, height: 900 });
    await openAudit();
    const o = await page.evaluate(() => document.documentElement.scrollWidth
      - document.documentElement.clientWidth);
    check('no horizontal overflow at ' + w, o <= 2, 'overflow ' + o);
  }

  await browser.close();
  server.close();
  console.log('\n' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
