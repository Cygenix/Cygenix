// Tests server-side delivery of run notifications.
//
// The gap this covers: the Webhook and Email (SMTP) connectors lived entirely
// in the browser — configuration in localStorage, dispatch from a Run-now
// poll loop in dashboard.html. A scheduled run fires in Azure with no browser
// anywhere, so it could never deliver to them, whatever the switches on
// Settings → Notifications said. azure-function/src/connectors.js is the
// server-side path; netlify/functions/scheduler.js stores the settings it
// reads.
//
// Two things matter more than the happy path and are asserted hardest:
//   * gating — an account that has not opted in must have nothing sent on
//     its behalf, because opting in is what moves an SMTP password out of
//     the browser.
//   * secrets — the password must never come back down to the browser and
//     must never reach the notification log, which is now shown in the UI.
const fs   = require('fs');
const path = require('path');
const Module = require('module');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

// connectors.js writes every attempt to Cosmos through notify-log.js. Stub
// the module so the log is inspectable and no client is ever constructed.
const logPath = path.join(__dirname, '..', 'azure-function', 'src', 'notify-log.js');
const logged = [];
require.cache[logPath] = new Module(logPath, null);
require.cache[logPath].filename = logPath;
require.cache[logPath].loaded = true;
require.cache[logPath].exports = {
  logNotification: async (row) => { logged.push(row); return row; },
};

const C = require(path.join(__dirname, '..', 'azure-function', 'src', 'connectors.js'));
const S = require(path.join(__dirname, '..', 'netlify', 'functions', 'scheduler.js'));

// A settings document as Cosmos would return it, and a container stub that
// serves it from the one item id connectors.js reads.
function containerWith(doc) {
  return {
    item: (id, pk) => ({
      read: async () => {
        if (id !== C.SETTINGS_ID) { const e = new Error('not found'); e.code = 404; throw e; }
        if (doc === null)         { const e = new Error('not found'); e.code = 404; throw e; }
        return { resource: { ...doc, id, userId: pk } };
      },
    }),
    items: {
      upsert: async (d) => ({ resource: d }),
      query: () => ({ fetchAll: async () => ({ resources: [] }) }),
    },
  };
}

// connectors.js builds its Cosmos client lazily from @azure/cosmos, so the
// stub has to be in the cache before the first call.
const cosmosPath = require.resolve('@azure/cosmos');
let _served = null;
require.cache[cosmosPath] = new Module(cosmosPath, null);
require.cache[cosmosPath].filename = cosmosPath;
require.cache[cosmosPath].loaded = true;
require.cache[cosmosPath].exports = {
  CosmosClient: class {
    database() { return { container: () => _served }; }
  },
};
const serve = (doc) => { _served = containerWith(doc); };

const SETTINGS = {
  docType: 'notify-settings',
  serverDelivery: true,
  events: { started: false, completed: true, failed: true },
  webhook: { enabled: true, url: 'https://hook.example/cygenix', secret: 'SHH' },
  smtp:    { enabled: false, host: 'smtp.example.com', port: 587, user: 'u@example.com',
             pass: 'PW', from: '', to: 'ops@example.com' },
};

const PAYLOAD = { project: 'Conversion', job: 'Nightly load', status: 'success',
                  rows: 1234, steps: 3, runId: 'run_1', message: '' };

const withFetch = async (impl, fn) => {
  const real = global.fetch;
  global.fetch = impl;
  try { return await fn(); } finally { global.fetch = real; }
};

console.log('Scheduled-run notifications — server-side connector delivery\n');

(async () => {
  // ── Gating ─────────────────────────────────────────────────────────────
  // Nothing may be sent on an account's behalf until the user has opted in,
  // because opting in is what put their SMTP password on the server.
  {
    serve(null);
    const r = await C.dispatchConnectors('u@example.com', 'completed', PAYLOAD, null);
    check('an account with no settings sends nothing', r.results.length === 0 && !!r.skipped, r.skipped);

    serve({ ...SETTINGS, serverDelivery: false });
    const off = await C.dispatchConnectors('u@example.com', 'completed', PAYLOAD, null);
    check('server delivery switched off sends nothing',
      off.results.length === 0 && /server delivery is off/.test(off.skipped), off.skipped);

    serve({ ...SETTINGS, events: { completed: false } });
    const ev = await C.dispatchConnectors('u@example.com', 'completed', PAYLOAD, null);
    check('an event switched off sends nothing',
      ev.results.length === 0 && /switched off/.test(ev.skipped), ev.skipped);

    // 'started' defaults to off, and a settings doc that predates the switch
    // must inherit that default rather than start emitting.
    serve({ ...SETTINGS, events: {} });
    const st = await C.dispatchConnectors('u@example.com', 'started', PAYLOAD, null);
    check('started is off unless it was turned on', st.results.length === 0, st.skipped);
    const done = await C.dispatchConnectors('u@example.com', 'completed', PAYLOAD, null);
    check('completed is on by default, matching the browser', done.results.length === 1);

    serve({ ...SETTINGS, webhook: { enabled: false }, smtp: { enabled: false } });
    const none = await C.dispatchConnectors('u@example.com', 'completed', PAYLOAD, null);
    check('no connector enabled sends nothing',
      none.results.length === 0 && /no connector/.test(none.skipped), none.skipped);

    const bogus = await C.dispatchConnectors('u@example.com', 'nonsense', PAYLOAD, null);
    check('an unknown event is refused rather than guessed', !!bogus.skipped);
  }

  // ── Webhook delivery ───────────────────────────────────────────────────
  {
    serve(SETTINGS);
    logged.length = 0;
    const calls = [];
    await withFetch(async (url, opts) => {
      calls.push({ url, opts, body: JSON.parse(opts.body) });
      return { ok: true, status: 200 };
    }, async () => {
      const r = await C.dispatchConnectors('u@example.com', 'completed', PAYLOAD, null);
      check('the webhook is delivered', r.results[0] && r.results[0].ok === true,
        r.results[0] && r.results[0].message);
    });
    check('it posts to the configured URL', calls[0].url === 'https://hook.example/cygenix', calls[0].url);
    check('the shared secret goes in the signature header',
      calls[0].opts.headers['X-Cygenix-Signature'] === 'SHH');
    // A receiver must not be able to tell a scheduled run from a Run-now, or
    // it would need two code paths for the same thing.
    check('the event name matches the one the browser sends',
      calls[0].body.event === 'cygenix.migration.success', calls[0].body.event);
    check('the run detail comes through',
      calls[0].body.rows === 1234 && calls[0].body.job === 'Nightly load' && calls[0].body.runId === 'run_1');
    check('a failure event maps to the failed name',
      C.EVENT_NAMES.failed === 'cygenix.migration.failed');
    check('the delivery is logged as sent',
      logged.length === 1 && logged[0].status === 'sent' && logged[0].channel === 'webhook',
      JSON.stringify(logged[0]));
  }

  // ── Failures are recorded, never thrown ────────────────────────────────
  // A dead webhook is not a migration failure. It must also not be silent,
  // or the run history says "success" and the user is never told nothing
  // was delivered.
  {
    serve(SETTINGS);
    logged.length = 0;
    await withFetch(async () => ({ ok: false, status: 500 }), async () => {
      const r = await C.dispatchConnectors('u@example.com', 'completed', PAYLOAD, null);
      check('an HTTP error is reported, not thrown', r.results[0].ok === false);
    });
    check('and written to the log as failed',
      logged.length === 1 && logged[0].status === 'failed' && /500/.test(logged[0].error),
      JSON.stringify(logged[0]));

    logged.length = 0;
    await withFetch(async () => { throw new Error('getaddrinfo ENOTFOUND'); }, async () => {
      const r = await C.dispatchConnectors('u@example.com', 'completed', PAYLOAD, null);
      check('an unreachable endpoint is reported, not thrown',
        r.results[0].ok === false && /ENOTFOUND/.test(r.results[0].message));
    });
    check('a network failure is logged too', logged.length === 1 && logged[0].status === 'failed');
  }

  // ── SMTP: the rules from send-email.js, which matter more unattended ────
  {
    check('a non-SMTP port is refused (this would be a port scanner)',
      /not an SMTP port/.test(C.validateSmtp({ host: 'h', port: 8080, user: 'u', pass: 'p', to: 't@e.com' })));
    check('587 is accepted', C.validateSmtp({ host: 'h', port: 587, user: 'u', pass: 'p', to: 't@e.com' }) === null);
    check('465 is accepted', C.validateSmtp({ host: 'h', port: 465, user: 'u', pass: 'p', to: 't@e.com' }) === null);
    check('a URL where a hostname belongs is refused',
      /not a URL/.test(C.validateSmtp({ host: 'https://smtp.example.com', port: 587, user: 'u', pass: 'p', to: 't@e.com' })));
    check('no recipient is refused before a socket opens',
      /No recipient/.test(C.validateSmtp({ host: 'h', port: 587, user: 'u', pass: 'p', to: '' })));
    check('a missing password is refused',
      /password/.test(C.validateSmtp({ host: 'h', port: 587, user: 'u', pass: '', to: 't@e.com' })));

    // A newline in an address closes the header and starts another one.
    check('CRLF is stripped from addresses',
      C.cleanAddr('a@b.com\r\nBcc: victim@c.com') === 'a@b.com Bcc: victim@c.com',
      C.cleanAddr('a@b.com\r\nBcc: victim@c.com'));
    check('and from the subject line',
      !/[\r\n]/.test(C.cleanAddr('Cygenix\nX-Injected: 1')));

    // SMTP servers echo the login back in rejections ("535 5.7.8 Username and
    // Password not accepted"), and this text is now shown in the UI.
    check('the password is scrubbed out of an error before it is stored',
      C.redact('535 auth failed for PW', 'PW') === '535 auth failed for ***');

    const f = C.smtpFormat({ event: 'cygenix.migration.failed', status: 'failed',
                             project: 'P', job: 'J', rows: 0, message: 'boom' });
    check('the subject names the event and the outcome',
      /cygenix\.migration\.failed/.test(f.subject) && /failed/.test(f.subject), f.subject);
    check('the body carries the project, job and error', /P/.test(f.text) && /J/.test(f.text) && /boom/.test(f.text));

    // Delivery itself: validation failures must be reported and logged
    // exactly like transport failures, not swallowed as "nothing to do".
    serve({ ...SETTINGS, webhook: { enabled: false },
            smtp: { ...SETTINGS.smtp, enabled: true, port: 9999 } });
    logged.length = 0;
    const r = await C.dispatchConnectors('u@example.com', 'completed', PAYLOAD, null);
    check('a misconfigured SMTP connector reports rather than silently skipping',
      r.results.length === 1 && r.results[0].ok === false, JSON.stringify(r.results));
    check('and it appears in the log', logged.length === 1 && logged[0].channel === 'smtp');
    check('the log never contains the password',
      !JSON.stringify(logged[0]).includes('PW'), JSON.stringify(logged[0]));
  }

  // ── The account copy: what goes up, and what comes back ────────────────
  {
    const masked = S.maskNotifySettings({
      serverDelivery: true,
      events: { completed: true },
      webhook: { enabled: true, url: 'https://h/x', secret: 'SHH' },
      smtp: { enabled: true, host: 'smtp.example.com', port: 587, user: 'u', pass: 'PW', to: 't@e.com' },
    });
    check('the SMTP password is never returned to the browser', masked.smtp.pass === undefined,
      JSON.stringify(masked.smtp));
    check('nor is the webhook secret', masked.webhook.secret === undefined);
    check('but the UI can still tell one is stored',
      masked.smtp.passSet === true && masked.webhook.secretSet === true);
    check('the non-secret fields survive',
      masked.smtp.host === 'smtp.example.com' && masked.webhook.url === 'https://h/x');
    check('an unset event falls back to its default rather than undefined',
      masked.events.failed === true && masked.events.started === false);

    // Because the browser cannot read the secrets back, a save that omits
    // them has to mean "leave them alone" — otherwise every save from a
    // second browser would silently wipe the password.
    const store = { doc: {
      id: 'notify-settings', docType: 'notify-settings', userId: 'u@example.com',
      serverDelivery: true, events: { completed: true },
      webhook: { enabled: true, url: 'https://h/x', secret: 'SHH' },
      smtp: { enabled: true, host: 'smtp.example.com', port: '587', user: 'u', pass: 'PW', to: 't@e.com' },
    } };
    const containers = { schedules: {
      item: () => ({ read: async () => ({ resource: store.doc }) }),
      items: { upsert: async (d) => { store.doc = d; return { resource: d }; } },
    } };

    await S.saveNotifySettings('u@example.com', { settings: {
      serverDelivery: true, events: { completed: false },
      smtp: { enabled: true, host: 'smtp2.example.com' },
    } }, containers);
    check('a save that omits the password keeps the stored one', store.doc.smtp.pass === 'PW');
    check('a save that omits the webhook secret keeps it', store.doc.webhook.secret === 'SHH');
    check('the fields that were sent are updated', store.doc.smtp.host === 'smtp2.example.com');
    check('and the event switch is updated', store.doc.events.completed === false);
    check('the document is tagged so it is not mistaken for a schedule',
      store.doc.docType === 'notify-settings' && store.doc.id === 'notify-settings');

    // Turning the feature off must not leave the credentials sitting there.
    await S.saveNotifySettings('u@example.com', { settings: {
      serverDelivery: false,
      webhook: { enabled: false, url: '', secret: '' },
      smtp: { enabled: false, host: '', port: '', user: '', pass: '', from: '', to: '' },
    } }, containers);
    check('opting out clears the stored credentials',
      store.doc.smtp.pass === '' && store.doc.webhook.secret === '' && store.doc.serverDelivery === false,
      JSON.stringify(store.doc.smtp));
  }

  // ── The settings document must not look like a schedule ────────────────
  {
    let asked = null;
    const containers = { schedules: { items: { query: (q) => { asked = q; return { fetchAll: async () => ({ resources: [] }) }; } } } };
    await S.listSchedules('u@example.com', {}, containers);
    check('the schedule list filters the settings document out',
      /docType/.test(asked.query), asked.query);

    const az = fs.readFileSync(path.join(__dirname, '..', 'azure-function', 'src', 'schedules.js'), 'utf8');
    check('and so does the copy in the Function App',
      /c\.userId = @uid[\s\S]{0,200}docType/.test(az));
  }

  // ── The log reader ─────────────────────────────────────────────────────
  {
    const rows = [{ id: '1', type: 'migration-success', to: 'a@b.com', status: 'sent', sentAt: 'x' }];
    const containers = { notifications: { items: { query: () => ({ fetchAll: async () => ({ resources: rows }) }) } } };
    const out = await S.listNotifications('u@example.com', {}, containers);
    check('log rows come back for the UI', out.notifications.length === 1);
    check('a row written before connectors existed reads as the built-in email',
      out.notifications[0].channel === 'resend', out.notifications[0].channel);

    const missing = { notifications: { items: { query: () => ({ fetchAll: async () => {
      const e = new Error('Owner resource does not exist'); e.code = 404; throw e;
    } }) } } };
    const gone = await S.listNotifications('u@example.com', {}, missing);
    check('a missing container explains itself instead of 500ing',
      gone.notifications.length === 0 && /notifications container/.test(gone.unavailable || ''),
      gone.unavailable);
  }

  // ── Wiring ─────────────────────────────────────────────────────────────
  // The dispatch has to be called from the runner, at the points where a run
  // actually reaches those states — the module being correct is no use if
  // nothing invokes it.
  {
    const rm = fs.readFileSync(path.join(__dirname, '..', 'azure-function', 'src', 'run-migration.js'), 'utf8');
    check('the runner imports the dispatcher', /require\('\.\/connectors'\)/.test(rm));
    check('it fires on the completed / failed terminal state',
      /dispatchConnectors\(userId, finalStatus === 'success' \? 'completed' : 'failed'/.test(rm));
    check('it fires on run start', /dispatchConnectors\(userId, 'started'/.test(rm));
    // Validation failures never reach the terminal block — they are exactly
    // the runs a user is otherwise never told about.
    check('and on the early-failure path too', /dispatchConnectors\(run\.userId, 'failed'/.test(rm));

    const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'azure-function', 'package.json'), 'utf8'));
    check('nodemailer is a dependency of the Function App, so SMTP can send',
      !!pkg.dependencies.nodemailer);
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})();
