// Tests the Settings → Notifications panel that opts an account into
// server-side delivery, and the delivery log shown beneath it.
//
// The connectors live in this browser. A scheduled run fires in Azure with no
// browser anywhere, so the only way it can deliver to them is if the settings
// are copied to the account — which moves an SMTP password out of localStorage
// and onto a server. That is a decision the user has to make knowingly, so
// this file pins both halves: that nothing leaves the browser until the switch
// is on, and that the switch says what it does.
//
// The code is extracted from public/dashboard.html and run against stubs, so
// what is under test is what ships.
const fs = require('fs');
const vm = require('vm');

const html  = (fs.readFileSync(__dirname + '/../public/dashboard.html', 'utf8') + '\n' + fs.readFileSync(__dirname + '/../public/dashboard-app.js', 'utf8'));
const start = html.indexOf('// ── Server-side delivery ─');
const end   = html.indexOf('// ── Which run events are allowed to notify ─');
if (start < 0 || end < 0 || end < start) {
  console.log('FAIL: could not locate the server-delivery module in dashboard.html');
  process.exit(1);
}

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

const SMTP_CFG = { host: 'smtp.example.com', port: '587', user: 'u@example.com',
                   pass: 'PW', from: '', to: 'ops@example.com', enabled: true };
const HOOK_CFG = { url: 'https://hook.example/x', secret: 'SHH', enabled: true };

function build(opts = {}) {
  const els = {};
  const el = (id) => (els[id] || (els[id] = { id, innerHTML: '', textContent: '', style: {} }));
  const calls = [];
  const enabled = opts.enabled || {};
  const sb = {
    console: { warn(){}, error(){} },
    JSON, String, Object, Promise, Array, Number, Date, Boolean,
    escIn: (s) => String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'),
    notifyEvents: () => opts.events || { started: false, completed: true, failed: true },
    document: { getElementById: (id) => (opts.noDom ? null : el(id)) },
    ta_sched: async (action, body) => {
      calls.push({ action, body });
      if (opts.apiThrows) throw new Error('HTTP 500');
      if (action === 'get-notify-settings')  return { settings: opts.settings || null };
      if (action === 'save-notify-settings') return { settings: { ...body.settings, updatedAt: '2026-08-15T10:00:00Z' } };
      if (action === 'list-notifications')   return opts.log || { notifications: [] };
      return {};
    },
    window: {
      CygenixIntegrations: opts.noIntegrations ? undefined : {
        getConfig: (id) => (id === 'smtp' ? SMTP_CFG : HOOK_CFG),
        isEnabled: (id) => !!enabled[id],
      },
    },
  };
  vm.createContext(sb);
  vm.runInContext(html.slice(start, end), sb);
  return { sb, els, calls, get: e => vm.runInContext(e, sb) };
}

console.log('Settings → Notifications — server-side delivery and the log\n');

(async () => {
  // ── Nothing leaves the browser until the switch is on ───────────────────
  // This is the whole point of the opt-in. If the payload carried the SMTP
  // password whenever the page was open, the switch would be decoration and
  // the Integrations page's promise would be false.
  {
    const t = build({ enabled: { smtp: true, webhook: true } });
    const off = t.get('notifySettingsPayload(false)');
    check('opting out sends no SMTP password', off.smtp.pass === '', JSON.stringify(off.smtp));
    check('opting out sends no SMTP host or recipient',
      off.smtp.host === '' && off.smtp.to === '');
    check('opting out sends no webhook URL or secret',
      off.webhook.url === '' && off.webhook.secret === '');
    check('and marks both connectors off, so a stale account copy stops sending',
      off.smtp.enabled === false && off.webhook.enabled === false);
    check('the flag itself is off', off.serverDelivery === false);

    const on = t.get('notifySettingsPayload(true)');
    check('opting in sends the SMTP settings the server needs',
      on.smtp.host === 'smtp.example.com' && on.smtp.pass === 'PW' && on.smtp.to === 'ops@example.com',
      JSON.stringify(on.smtp));
    check('and the webhook URL and secret', on.webhook.url === 'https://hook.example/x' && on.webhook.secret === 'SHH');
    check('and the event switches, so the server honours the same rules',
      on.events.completed === true && on.events.started === false);
  }

  // A connector that is switched off in Integrations must not be enabled on
  // the server just because the account opted in.
  {
    const t = build({ enabled: { webhook: true } });
    const on = t.get('notifySettingsPayload(true)');
    check('a connector disabled in Integrations stays disabled on the server',
      on.smtp.enabled === false && on.webhook.enabled === true);
  }

  // ── The switch does what it says ───────────────────────────────────────
  {
    const t = build({ enabled: { smtp: true } });
    await t.get('onServerDeliveryToggle(true)');
    const save = t.calls.find(c => c.action === 'save-notify-settings');
    check('turning it on uploads immediately rather than waiting for a save',
      !!save && save.body.settings.serverDelivery === true);
    check('and the upload carries the credentials', save.body.settings.smtp.pass === 'PW');

    const t2 = build({ enabled: { smtp: true }, settings: { serverDelivery: true, smtp: {}, webhook: {} } });
    await t2.get('onServerDeliveryToggle(false)');
    const off = t2.calls.find(c => c.action === 'save-notify-settings');
    check('turning it off uploads the cleared settings, rather than leaving them on the server',
      !!off && off.body.settings.serverDelivery === false && off.body.settings.smtp.pass === '');
  }

  // ── The disclosure ─────────────────────────────────────────────────────
  // A user cannot consent to something the page does not tell them. If this
  // assertion ever fails because the copy was trimmed, the copy is wrong.
  {
    const t = build({ settings: { serverDelivery: false, events: {}, smtp: {}, webhook: {} } });
    await t.get('loadNotifyServer()');
    const h = t.els['nt-server'].innerHTML;
    check('the panel says the SMTP password leaves the browser',
      /SMTP password/i.test(h) && /leave this browser|leave the browser/i.test(h), h.slice(0, 200));
    check('and names the webhook secret too', /webhook secret/i.test(h));
    // The account is the source of truth for this switch, not localStorage —
    // a second browser must not show it off while the server is delivering.
    check('the switch reads its state from the account, and is off here',
      /id="nt-server-on"/.test(h) && !/id="nt-server-on"\s+checked/.test(h));

    const on = build({ settings: { serverDelivery: true, events: {}, smtp: {}, webhook: {} } });
    await on.get('loadNotifyServer()');
    check('and shows on when the account says so',
      /id="nt-server-on"\s+checked/.test(on.els['nt-server'].innerHTML));
  }

  // ── Drift between the two copies is visible ────────────────────────────
  // Otherwise editing SMTP in Integrations silently leaves scheduled runs
  // using the old settings, which looks exactly like the feature being broken.
  {
    const t = build({
      enabled: { smtp: true, webhook: true },
      settings: { serverDelivery: true, events: {},
                  smtp: { enabled: true }, webhook: { enabled: false }, updatedAt: '2026-08-01T00:00:00Z' },
    });
    await t.get('loadNotifyServer()');
    const h = t.els['nt-server'].innerHTML;
    check('a mismatch between browser and account is called out',
      /does not match the account copy/.test(h), h.slice(-400));

    const t2 = build({
      enabled: { smtp: true },
      settings: { serverDelivery: true, events: {},
                  smtp: { enabled: true }, webhook: { enabled: false }, updatedAt: '2026-08-01T00:00:00Z' },
    });
    await t2.get('loadNotifyServer()');
    check('no warning when the two agree',
      !/does not match/.test(t2.els['nt-server'].innerHTML));
    check('and it names what the server will deliver to',
      /will deliver to/.test(t2.els['nt-server'].innerHTML));
  }

  // On, but nothing uploaded — the state that would otherwise be silent.
  {
    const t = build({ settings: { serverDelivery: true, events: {}, smtp: { enabled: false }, webhook: { enabled: false } } });
    await t.get('loadNotifyServer()');
    check('server delivery on with no connector says so plainly',
      /no connector was copied up/.test(t.els['nt-server'].innerHTML));
  }

  // ── Failures surface ───────────────────────────────────────────────────
  {
    const t = build({ apiThrows: true });
    await t.get('loadNotifyServer()');
    check('an unreachable settings API reports rather than showing a blank panel',
      /Could not read your account settings/.test(t.els['nt-server'].innerHTML));

    await t.get('loadNotifyLog()');
    check('and the log says so too', /Could not read the log/.test(t.els['nt-log'].innerHTML));
  }

  // ── The log ────────────────────────────────────────────────────────────
  {
    const t = build({ log: { notifications: [
      { id:'1', channel:'smtp',    type:'cygenix.migration.success', to:'ops@example.com',
        status:'sent',   error:'', source:'scheduled-run', sentAt:'2026-08-15T09:00:00Z' },
      { id:'2', channel:'webhook', type:'cygenix.migration.failed',  to:'https://hook.example/x',
        status:'failed', error:'HTTP 500', source:'scheduled-run', sentAt:'2026-08-15T08:00:00Z' },
      { id:'3', channel:'resend',  type:'migration-success', to:'me@example.com',
        status:'skipped', error:'RESEND_API_KEY not configured', sentAt:'2026-08-15T07:00:00Z' },
    ] } });
    await t.get('loadNotifyLog()');
    const h = t.els['nt-log'].innerHTML;
    check('a successful delivery is listed', /ops@example\.com/.test(h) && /sent/.test(h));
    // The reason a delivery failed is the only thing the user actually needs
    // from this panel.
    check('a failure shows its reason', /HTTP 500/.test(h));
    check('a skipped send shows its reason too', /RESEND_API_KEY not configured/.test(h));
    check('channels are named in plain English rather than by id',
      /Email \(SMTP\)/.test(h) && /Webhook/.test(h));

    const empty = build({ log: { notifications: [] } });
    await empty.get('loadNotifyLog()');
    check('an empty log explains what would appear there',
      /Nothing yet/.test(empty.els['nt-log'].innerHTML));

    const missing = build({ log: { notifications: [], unavailable: 'The notifications container does not exist.' } });
    await missing.get('loadNotifyLog()');
    check('a missing container passes the setup message through',
      /container does not exist/.test(missing.els['nt-log'].innerHTML));
  }

  // Log rows carry text from a mail server's rejection, which is not ours to
  // trust — it lands in innerHTML.
  {
    const t = build({ log: { notifications: [
      { id:'x', channel:'smtp', type:'t', to:'a@b.com', status:'failed',
        error:'<img src=x onerror=alert(1)>', sentAt:'2026-08-15T09:00:00Z' },
    ] } });
    await t.get('loadNotifyLog()');
    const h = t.els['nt-log'].innerHTML;
    check('an error message from a mail server is escaped, not rendered',
      !/<img/.test(h) && /&lt;img/.test(h), h.slice(0, 300));
  }

  // ── Nothing loaded ─────────────────────────────────────────────────────
  {
    const t = build({ noIntegrations: true });
    const p = t.get('notifySettingsPayload(true)');
    check('no integrations module yields an empty payload rather than a crash',
      p.smtp.host === '' && p.webhook.url === '');

    const nd = build({ noDom: true });
    await nd.get('loadNotifyServer()');
    await nd.get('loadNotifyLog()');
    check('rendering with the panel absent is a no-op, not a crash', true);
  }

  // ── Element ids are unique across the page ─────────────────────────────
  // getElementById returns the FIRST match, so a duplicate id silently hands
  // code the wrong element — the failure mode that once made rendering the
  // GROUP BY rows wipe the source column list. This panel adds four ids to a
  // twenty-thousand-line page, so the check belongs here.
  //
  // Two ids appear twice in the source without being duplicates in the DOM:
  // they sit in the two mutually exclusive branches of the integration modal
  // template, and openIntegrationModal removes the existing overlay before
  // inserting either. A text scan cannot tell that apart, so they are named
  // here rather than weakening the check for everything else.
  {
    const TEMPLATE_BRANCHES = new Set(['in-enable-toggle', 'in-modal-overlay']);
    const ids = [...html.matchAll(/\sid="([^"]+)"/g)].map(m => m[1]);
    const seen = new Set(), dupes = new Set();
    ids.forEach(id => { if (seen.has(id)) dupes.add(id); else seen.add(id); });
    TEMPLATE_BRANCHES.forEach(id => dupes.delete(id));
    check('no element id is used twice in dashboard.html', dupes.size === 0, [...dupes].join(', '));
    check('the new panels are on the page',
      ['nt-server', 'nt-log', 'nt-server-on', 'nt-server-result'].every(id => seen.has(id) || html.includes('id="' + id + '"')));
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})();
