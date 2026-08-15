// Tests the Email (SMTP) integration, both halves.
//
// PART 1 runs netlify/functions/send-email.js against a real SMTP server
// listening on localhost, so delivery, authentication failure, port and host
// validation, header-injection defence and password redaction are all
// exercised for real rather than mocked.
//
// PART 2 runs the browser-side handlers extracted from cygenix-integrations.js
// against a stubbed fetch: config validation, the payload shape sent to the
// function, message formatting, and emit()'s isolation guarantee — a refused
// mail server must never turn a successful migration into a failure.
const fs = require('fs');
const vm = require('vm');
const path = require('path');
const Module = require('module');
const { SMTPServer } = require('smtp-server');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

// ── Load the function with its Entra verifier stubbed ────────────────────
// The auth path is covered by the other functions' own usage; what needs
// testing here is everything after the token check.
const FN_PATH = path.join(__dirname, '..', 'netlify', 'functions', 'send-email.js');
const realRequire = Module.prototype.require;
Module.prototype.require = function (id) {
  if (id === './lib/entra-auth') {
    return { verifyAuthHeader: async () => ({ email: 'tester@cygenix.co.uk' }) };
  }
  return realRequire.apply(this, arguments);
};
const fn = require(FN_PATH);
Module.prototype.require = realRequire;

const call = body => fn.handler({
  httpMethod: 'POST', headers: { authorization: 'Bearer stub' }, body: JSON.stringify(body),
});
const J = r => { try { return JSON.parse(r.body); } catch { return {}; } };

// ── PART 1 ───────────────────────────────────────────────────────────────
async function part1(port, received) {
  console.log('Email (SMTP) — the relay function\n');
  const smtp = { host: '127.0.0.1', port, user: 'good@example.com', pass: 'letmein' };

  // Validation happens before any socket is opened.
  const V = fn.validateSmtp;
  check('a complete config validates', V(smtp) === null, String(V(smtp)));
  check('a missing host is caught', /host is required/.test(V({ ...smtp, host: '' })));
  check('a missing port is caught', /port is required/.test(V({ ...smtp, port: undefined })));
  check('a missing username is caught', /username is required/.test(V({ ...smtp, user: '' })));
  check('a missing password is caught', /password is required/.test(V({ ...smtp, pass: '' })));
  check('nothing at all is caught', /settings missing/.test(V(null)));
  check('only real SMTP ports are allowed',
    [25, 465, 587, 2525].every(p => fn.ALLOWED_PORTS.has(p)) &&
    ![22, 80, 443, 3306, 6379, 8080].some(p => fn.ALLOWED_PORTS.has(p)));

  // Redaction — SMTP servers quote the login back in rejections.
  check('the password is redacted from text', fn.redact('login letmein failed', 'letmein') === 'login *** failed');
  check('every occurrence is redacted', fn.redact('letmein/letmein', 'letmein') === '***/***');
  check('redaction with no secret is a no-op', fn.redact('hello', '') === 'hello');
  check('redaction survives a null message', fn.redact(null, 'x') === '');

  // Addresses are headers — a newline is a header injection.
  check('a newline in an address is neutralised', fn.cleanAddr('a@b.c\r\nBcc: evil@x') === 'a@b.c Bcc: evil@x');
  check('an address is trimmed', fn.cleanAddr('  a@b.c  ') === 'a@b.c');

  // ── Live, against a real server ────────────────────────────────────────
  let r = await call({ action: 'verify', smtp });
  check('verify connects and authenticates', r.statusCode === 200 && J(r).ok === true, r.body);

  r = await call({ action: 'verify', smtp: { ...smtp, pass: 'wrongpass' } });
  check('a wrong password fails', r.statusCode === 502, r.body);
  check('the wrong password is not echoed back to the client',
    !r.body.includes('wrongpass'), r.body);

  r = await call({ action: 'send', smtp, to: 'ops@example.com', subject: 'Hello', text: 'Body here' });
  check('a message sends', r.statusCode === 200 && J(r).ok === true, r.body);
  check('one recipient is reported accepted', J(r).accepted === 1 && J(r).rejected === 0, r.body);
  check('the server really received it', received.length === 1 && /Body here/.test(received[0]));
  check('the subject arrived', /Subject: Hello/.test(received[0] || ''));
  check('the From address defaults to the SMTP username',
    /From: .*good@example\.com/.test(received[0] || ''), (received[0] || '').split('\n')[0]);

  r = await call({ action: 'send', smtp, to: 'a@x.com, b@y.com', subject: 'S', text: 'T' });
  check('comma-separated recipients all go', r.statusCode === 200 && J(r).accepted === 2, r.body);

  r = await call({ action: 'send', smtp, from: 'noreply@example.com', to: 'a@x.com', subject: 'S', text: 'T' });
  check('an explicit From is used', /From: .*noreply@example\.com/.test(received[received.length - 1] || ''));

  r = await call({ action: 'send', smtp, to: '', subject: 'S', text: 'T' });
  check('no recipient is a 400', r.statusCode === 400 && /recipient/.test(r.body), r.body);

  r = await call({ action: 'send', smtp, to: 'a@x.com', subject: 'S', text: '   ' });
  check('an empty body is a 400', r.statusCode === 400 && /empty/.test(r.body), r.body);

  // The SSRF guard: refuse before a socket is opened at all.
  r = await call({ action: 'send', smtp: { ...smtp, port: 8080 }, to: 'a@x.com', text: 'T' });
  check('a non-SMTP port is refused', r.statusCode === 400 && /not an SMTP port/.test(r.body), r.body);
  r = await call({ action: 'send', smtp: { ...smtp, host: 'https://smtp.example.com' }, to: 'a@x.com', text: 'T' });
  check('a URL pasted as the host is refused', r.statusCode === 400 && /hostname/.test(r.body), r.body);

  const before = received.length;
  await call({ action: 'send', smtp, to: 'a@x.com\r\nBcc: evil@example.com', subject: 'S', text: 'T' });
  const hdrs = (received[before] || '').split(/\r?\n/);
  check('a newline in the recipient cannot inject a Bcc header',
    !hdrs.some(l => /^Bcc:/i.test(l)), hdrs.slice(0, 6).join(' | '));

  r = await call({ action: 'nonsense', smtp });
  check('an unknown action is refused', r.statusCode === 400);
  r = await fn.handler({ httpMethod: 'GET' });
  check('GET is refused', r.statusCode === 405);
  r = await fn.handler({ httpMethod: 'OPTIONS' });
  check('OPTIONS preflight succeeds', r.statusCode === 200);
  r = await fn.handler({ httpMethod: 'POST', headers: { authorization: 'Bearer x' }, body: '{oops' });
  check('a malformed body is a 400', r.statusCode === 400 && /Invalid JSON/.test(r.body));
}

// ── PART 2 ───────────────────────────────────────────────────────────────
function part2() {
  console.log('\nEmail (SMTP) — the browser-side connector\n');

  const src = fs.readFileSync(path.join(__dirname, '..', 'public', 'cygenix-integrations.js'), 'utf8');
  const a = src.indexOf('  // ─── Email (SMTP) ─');
  const b = src.indexOf('  // JSON export — no config needed');
  if (a < 0 || b < 0) { check('could locate the SMTP handlers', false); return; }

  const calls = [];
  function build(reply) {
    const sb = {
      console, JSON, String, Number, Date, Promise,
      fetch: async (url, opts) => {
        calls.push({ url, body: JSON.parse(opts.body) });
        const r = reply || { ok: true, status: 200, json: { ok: true, accepted: 1, rejected: 0 } };
        return { ok: r.ok, status: r.status, json: async () => r.json };
      },
    };
    vm.createContext(sb);
    vm.runInContext(src.slice(a, b), sb);
    return { sb, get: e => vm.runInContext(e, sb) };
  }

  const CFG = { host:'smtp.example.com', port:'587', user:'me@example.com', pass:'pw', to:'ops@example.com' };

  // Config completeness — each gap names the field, so the dialog can say
  // which box to fill rather than "not configured".
  let t = build();
  const miss = c => t.get('smtpMissing(' + JSON.stringify(c) + ')');
  check('a complete config is not flagged', miss(CFG) === null, String(miss(CFG)));
  check('a missing host names the host', /host/.test(miss({ ...CFG, host:'' })));
  check('a missing port names the port', /port/.test(miss({ ...CFG, port:'' })));
  check('a missing password names the password', /password/.test(miss({ ...CFG, pass:'' })));
  check('no config at all is flagged', /not configured/.test(miss(null)));

  // The payload the function receives.
  t = build();
  const p = t.get('smtpPayload(' + JSON.stringify(CFG) + ')');
  check('the port is sent as a number, not the string from the form', p.smtp.port === 587);
  check('the credentials are passed through', p.smtp.user === 'me@example.com' && p.smtp.pass === 'pw');
  check('From falls back to the username when blank', p.from === 'me@example.com');
  const p2 = t.get('smtpPayload(' + JSON.stringify({ ...CFG, from:' bot@example.com ' }) + ')');
  check('an explicit From is trimmed and used', p2.from === 'bot@example.com');

  // Message formatting from an emitted event.
  t = build();
  const f = t.get('smtpFormat(' + JSON.stringify({
    event:'cygenix.migration.success', status:'success', project:'Demo', job:'Nightly', rows:1500,
  }) + ')');
  check('the subject names the event and outcome',
    /cygenix\.migration\.success/.test(f.subject) && /success/.test(f.subject), f.subject);
  check('the body carries the project, job and row count',
    /Demo/.test(f.text) && /Nightly/.test(f.text) && /1500/.test(f.text), f.text);
  const f2 = t.get("smtpFormat({ event:'x.y' })");
  check('an event with no detail still produces a readable message',
    /No further detail/.test(f2.text), f2.text);
  const f3 = t.get("smtpFormat({ event:'cygenix.migration.failed', status:'failed', message:'timeout' })");
  check('a failure carries its error text', /timeout/.test(f3.text), f3.text);

  // Test: verify first, then send — they fail for different reasons.
  calls.length = 0;
  t = build();
  let r = t.get('testSmtp(' + JSON.stringify(CFG) + ')');
  return r.then(res => {
    check('test verifies before sending', calls.length === 2 && calls[0].body.action === 'verify',
      calls.map(c => c.body.action).join(','));
    check('test then sends a message', calls[1] && calls[1].body.action === 'send');
    check('test posts to the relay function',
      calls[0].url === '/.netlify/functions/send-email', calls[0].url);
    check('test reports success with the recipient', res.ok && /ops@example\.com/.test(res.message), res.message);

    // With no recipient, verifying is all that can be proved — and it says so
    // rather than claiming a message was sent.
    calls.length = 0;
    const t2 = build();
    return t2.get('testSmtp(' + JSON.stringify({ ...CFG, to:'' }) + ')');
  }).then(res => {
    check('with no recipient, test stops after verifying',
      calls.length === 1 && res.ok && /Add a recipient/.test(res.message), res.message);

    // A rejected recipient is not a success.
    const t3 = build({ ok:true, status:200, json:{ ok:true, accepted:0, rejected:1 } });
    return t3.get('runSmtp(' + JSON.stringify(CFG) + ', { event:"e" })');
  }).then(res => {
    check('a rejected recipient is reported as a failure',
      res.ok === false && /rejected/.test(res.message), res.message);

    // A server error surfaces its message rather than a bare "failed".
    const t4 = build({ ok:false, status:502, json:{ error:'Invalid login: 535' } });
    return t4.get('runSmtp(' + JSON.stringify(CFG) + ', { event:"e" })');
  }).then(res => {
    check('a relay error surfaces the server message',
      res.ok === false && /535/.test(res.message), res.message);

    // Guard rails before any network call.
    calls.length = 0;
    const t5 = build();
    return t5.get('runSmtp(' + JSON.stringify({ ...CFG, to:'' }) + ', {})');
  }).then(res => {
    check('run refuses with no recipient and makes no request',
      res.ok === false && /recipient/.test(res.message) && calls.length === 0, res.message);
  });
}

// ── emit() isolation ─────────────────────────────────────────────────────
// A dead connector must never turn a successful migration into a failure.
function part3() {
  console.log('\nIntegrations — emit isolation\n');
  const src = fs.readFileSync(path.join(__dirname, '..', 'public', 'cygenix-integrations.js'), 'utf8');
  const a = src.indexOf('  // Broadcast an event to every enabled subscriber.');
  const b = src.indexOf('  window.CygenixIntegrations = {');
  if (a < 0 || b < 0) { check('could locate emit()', false); return Promise.resolve(); }

  const sb = {
    console: { warn: () => {}, error: () => {} }, JSON, String, Date, Promise, Array,
    isEnabled: id => id === 'webhook' || id === 'smtp',
    getConfig: () => ({}),
    runWebhook: async () => { throw new Error('network down'); },
    runSmtp:    async () => ({ ok:true, message:'sent' }),
  };
  vm.createContext(sb);
  vm.runInContext(src.slice(a, b), sb);
  return vm.runInContext("emit('cygenix.migration.success', { status:'success' })", sb).then(results => {
    check('a throwing connector does not reject the whole emit', Array.isArray(results));
    check('both enabled connectors are attempted', results.length === 2, String(results.length));
    check('the failing connector is reported, not thrown',
      results.some(r => r.id === 'webhook' && r.ok === false && /network down/.test(r.message)),
      JSON.stringify(results));
    check('the working connector still delivered',
      results.some(r => r.id === 'smtp' && r.ok === true), JSON.stringify(results));

    // A disabled connector is not contacted at all.
    const sb2 = { ...sb, isEnabled: () => false };
    vm.createContext(sb2);
    vm.runInContext(src.slice(a, b), sb2);
    return vm.runInContext("emit('e', {})", sb2);
  }).then(results => {
    check('nothing is sent when no connector is enabled', results.length === 0);
  });
}

// ── Run ──────────────────────────────────────────────────────────────────
// The function only permits real SMTP ports, so the test server has to listen
// on one — 2525 is in the allow-list and needs no privileges.
const PORT = 2525;
const received = [];
const server = new SMTPServer({
  authOptional: false,
  disabledCommands: ['STARTTLS'],
  onAuth(auth, s, cb) {
    if (auth.username === 'good@example.com' && auth.password === 'letmein') return cb(null, { user: auth.username });
    return cb(new Error('535 Authentication failed'));
  },
  onData(stream, s, cb) { let d = ''; stream.on('data', c => d += c); stream.on('end', () => { received.push(d); cb(); }); },
});
// Quiet: the bad-password case deliberately causes a connection error.
server.on('error', () => {});

server.on('error', e => {
  if (e && e.code === 'EADDRINUSE') {
    console.log('FAIL: port ' + PORT + ' is in use — cannot start the test SMTP server');
    process.exit(1);
  }
});

server.listen(PORT, '127.0.0.1', async () => {
  try {
    await part1(PORT, received);
    await part2();
    await part3();
  } catch (e) {
    check('the suite ran to completion', false, e && e.stack);
  }
  console.log(`\n${pass} passed, ${fail} failed`);
  server.close(() => process.exit(fail ? 1 : 0));
  // Belt and braces: a lingering keep-alive socket must not hang the suite.
  setTimeout(() => process.exit(fail ? 1 : 0), 500).unref();
});
