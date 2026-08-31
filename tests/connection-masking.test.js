// Tests the Connections view's masking of configured database connections.
//
// A connection string embeds the password in cleartext, and the view used to
// render it into a plain text input on open — so the credential was on screen
// for any screenshot, screen share or passer-by. The rule these tests hold to
// is narrow and absolute: while a connection is masked, nothing but the
// database name and the user's own nickname reaches the DOM.
//
// The masking module, the connection-string parser and the saved-chip tooltip
// are all extracted from public/dashboard.html and run against a DOM stub, so
// what is under test is the shipped code.
const fs = require('fs');
const vm = require('vm');

const html = (fs.readFileSync(__dirname + '/../public/dashboard.html', 'utf8') + '\n' + fs.readFileSync(__dirname + '/../public/dashboard-app.js', 'utf8'));

function slice(from, to, what) {
  const a = html.indexOf(from), b = html.indexOf(to);
  if (a < 0 || b < 0 || b < a) {
    console.log('FAIL: could not locate ' + what + ' in dashboard.html');
    process.exit(1);
  }
  return html.slice(a, b);
}

const parserSrc  = slice('function parseDbConnection(connStr){',
                         '// Read whatever connection info is currently configured', 'parseDbConnection');
const maskingSrc = slice('// ── Connection masking ─',
                         'function initConnectionsView()', 'the connection-masking module');
const previewSrc = slice('// Tooltip text for a saved-connection chip.',
                         '// Dialect label for the chip', 'sconnPreview');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

// ── Minimal DOM ──────────────────────────────────────────────────────────
// Only what the module touches: display, textContent, value, classList, focus.
function makeEl(id) {
  return {
    id, value: '', textContent: '',
    style: { display: '' },
    _classes: new Set(),
    classList: {
      toggle(c, on) { on ? this._owner._classes.add(c) : this._owner._classes.delete(c); },
    },
    focus() { this.focused = true; },
  };
}

const IDS = ['src','tgt'].flatMap(s => [
  s + '-conn-locked', s + '-conn-locked-label', s + '-conn-locked-value',
  s + '-conn-locked-nick', s + '-conn-hide',
  s + '-direct-wrap', s + '-azure-wrap',
  s + '-build-wrap', s + '-cs-wrap', s + '-entry-paste', s + '-entry-build', s + '-b-host',
  'proj-' + s + '-cs', 'proj-' + s + '-fn-url', 'proj-' + s + '-fn-key',
]);

// Builds a sandbox with the shipped code loaded and the fields pre-filled.
// `saved` stands in for the user's saved-connection list.
function build({ srcMode = 'direct', tgtMode = 'azure', fields = {}, saved = [] } = {}) {
  const els = {};
  IDS.forEach(id => { els[id] = makeEl(id); els[id].classList._owner = els[id]; });
  Object.entries(fields).forEach(([id, v]) => { if (els[id]) els[id].value = v; });

  const sb = {
    console, JSON, String, Array, Number, RegExp, URL,
    window: {},
    document: { getElementById: id => els[id] || null },
    sconnGetAll: () => saved,
    srcMode, tgtMode,
  };
  vm.createContext(sb);
  vm.runInContext(parserSrc + '\n' + maskingSrc + '\n' + previewSrc, sb);
  return { sb, els, call: (fn, arg) => vm.runInContext(fn + '(' + JSON.stringify(arg) + ')', sb) };
}

const shown = el => el.style.display !== 'none';
// Everything the masked card puts on screen, as one string.
const cardText = els => [
  els['src-conn-locked-label'], els['src-conn-locked-value'], els['src-conn-locked-nick'],
].map(e => e.textContent).join(' | ');

console.log('Connections view — connection masking\n');

// A real string of the shape the user has configured, password and all.
const CS = 'mssql://mtvne:jY4mN7xP2w@86.20.94.140:1433/Conversion_DM?encrypt=true&trustServerCertificate=true';

// ── A configured connection is masked ────────────────────────────────────
let t = build({ fields: { 'proj-src-cs': CS } });
t.call('renderConnLock', 'src');

check('a configured connection shows the summary card', shown(t.els['src-conn-locked']));
check('a configured connection hides the connection-string field',
  !shown(t.els['src-direct-wrap']));
check('the summary shows the database name',
  t.els['src-conn-locked-value'].textContent === 'Conversion_DM',
  t.els['src-conn-locked-value'].textContent);

// The point of the whole change.
const leaked = cardText(t.els);
check('the password never reaches the card', !leaked.includes('jY4mN7xP2w'), leaked);
check('the host never reaches the card', !leaked.includes('86.20.94.140'), leaked);
check('the username never reaches the card', !/\bmtvne\b/.test(leaked), leaked);
check('the port never reaches the card', !leaked.includes('1433'), leaked);

// ── Nicknames ────────────────────────────────────────────────────────────
t = build({ fields: { 'proj-src-cs': CS },
            saved: [{ side:'src', mode:'direct', connString: CS, name:'Conversion' }] });
t.call('renderConnLock', 'src');
check('a matching saved connection contributes its nickname',
  t.els['src-conn-locked-nick'].textContent === 'Conversion' &&
  shown(t.els['src-conn-locked-nick']));

// A saved entry for the *other* side, or a different string, is not a match.
t = build({ fields: { 'proj-src-cs': CS },
            saved: [{ side:'tgt', mode:'direct', connString: CS, name:'Wrong side' },
                    { side:'src', mode:'direct', connString:'mssql://a:b@h/other', name:'Other DB' }] });
t.call('renderConnLock', 'src');
check('an unmatched connection shows no nickname',
  t.els['src-conn-locked-nick'].textContent === '' &&
  !shown(t.els['src-conn-locked-nick']));

// ── Azure Function mode ──────────────────────────────────────────────────
const FN = 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/db';
t = build({ tgtMode: 'azure', fields: { 'proj-tgt-fn-url': FN, 'proj-tgt-fn-key': 'secretkey' },
            saved: [{ side:'tgt', mode:'azure', fnUrl: FN, name:'cygenix-cloud_new' }] });
t.call('renderConnLock', 'tgt');
check('an Azure Function connection is masked too', shown(t.els['tgt-conn-locked']));
check('the Azure Function fields are hidden', !shown(t.els['tgt-azure-wrap']));
const tgtText = [t.els['tgt-conn-locked-label'], t.els['tgt-conn-locked-value'],
                 t.els['tgt-conn-locked-nick']].map(e => e.textContent).join(' | ');
check('the function host is not shown', !tgtText.includes('azurewebsites.net'), tgtText);
check('the function key is not shown', !tgtText.includes('secretkey'), tgtText);
check('the Azure summary is identified by its nickname',
  t.els['tgt-conn-locked-nick'].textContent === 'cygenix-cloud_new');

// ── Nothing configured → fields stay visible ─────────────────────────────
t = build();
t.call('renderConnLock', 'src');
check('an unconfigured side shows no card', !shown(t.els['src-conn-locked']));
check('an unconfigured side shows its fields so it can be filled in',
  shown(t.els['src-direct-wrap']));
check('an unconfigured side offers no Hide button', !shown(t.els['src-conn-hide']));

// ── Reveal / hide ────────────────────────────────────────────────────────
t = build({ fields: { 'proj-src-cs': CS } });
t.call('renderConnLock', 'src');
t.call('connReveal', 'src');
check('revealing shows the connection-string field', shown(t.els['src-direct-wrap']));
check('revealing hides the summary card', !shown(t.els['src-conn-locked']));
check('revealing offers a way to hide again', shown(t.els['src-conn-hide']));
// Edit opens Settings — the form — so the cursor belongs in its first field.
// It used to land in the connection-string input, which is now hidden behind
// the form; focusing a hidden input is the kind of thing that looks like
// nothing happened.
check('revealing puts the cursor in the first field of the form',
  t.els['src-b-host'].focused === true);
check('and the raw string field is not what is shown',
  t.els['src-cs-wrap'].style.display === 'none');
check('while the form itself is', shown(t.els['src-build-wrap']));

t.call('connHide', 'src');
check('hiding restores the summary card', shown(t.els['src-conn-locked']));
check('hiding puts the connection string back out of sight',
  !shown(t.els['src-direct-wrap']));

// Revealing one side must not reveal the other.
t = build({ tgtMode: 'direct', fields: { 'proj-src-cs': CS, 'proj-tgt-cs': CS } });
t.call('renderConnLock', 'src');
t.call('renderConnLock', 'tgt');
t.call('connReveal', 'src');
check('revealing the source leaves the target masked',
  !shown(t.els['tgt-direct-wrap']) && shown(t.els['tgt-conn-locked']));

// ── Mode switching cannot leak ───────────────────────────────────────────
// setSrcMode delegates to renderConnLock rather than toggling the wraps
// itself, so it cannot put a configured value on screen. With both modes
// configured — which happens as soon as a user has tried each — switching
// between them leaves both masked.
t = build({ fields: { 'proj-src-cs': CS, 'proj-src-fn-url': FN } });
t.call('renderConnLock', 'src');
t.call('setSrcMode', 'azure');
check('switching to a configured azure mode stays masked',
  !shown(t.els['src-azure-wrap']) && shown(t.els['src-conn-locked']));
t.call('setSrcMode', 'direct');
check('switching back to a configured direct mode stays masked',
  !shown(t.els['src-direct-wrap']) && shown(t.els['src-conn-locked']));

// Switching to a mode that has nothing configured shows empty fields — there
// is no secret to withhold, and the user needs somewhere to type.
t = build({ fields: { 'proj-src-cs': CS } });
t.call('renderConnLock', 'src');
t.call('setSrcMode', 'azure');
check('switching to an unconfigured mode offers its empty fields',
  shown(t.els['src-azure-wrap']) && t.els['proj-src-fn-url'].value === '');
check('the configured string is not on screen in the other mode',
  !shown(t.els['src-direct-wrap']));
t.call('setSrcMode', 'direct');
check('returning to the configured mode re-masks it',
  !shown(t.els['src-direct-wrap']) && shown(t.els['src-conn-locked']));

// Once revealed, the mode toggle behaves normally.
t.call('connReveal', 'src');
t.call('setSrcMode', 'azure');
check('a revealed side shows the fields for the mode it switched to',
  shown(t.els['src-azure-wrap']) && !shown(t.els['src-direct-wrap']));

// ── Degenerate connection strings ────────────────────────────────────────
// A string with no database segment is still configured — it must not fall
// back to showing the raw string.
t = build({ fields: { 'proj-src-cs': 'Server=tcp:host.database.windows.net,1433;User Id=sa;Password=hunter2;' } });
t.call('renderConnLock', 'src');
check('a string with no database name still masks', shown(t.els['src-conn-locked']));
check('a string with no database name says so instead of leaking',
  t.els['src-conn-locked-value'].textContent === 'Configured',
  t.els['src-conn-locked-value'].textContent);
check('no password escapes via the fallback',
  !cardText(t.els).includes('hunter2'));

// Whitespace-only is not a configured connection.
t = build({ fields: { 'proj-src-cs': '   ' } });
t.call('renderConnLock', 'src');
check('a blank connection string counts as unconfigured',
  !shown(t.els['src-conn-locked']) && shown(t.els['src-direct-wrap']));

// An unparseable string must mask rather than fall open.
t = build({ fields: { 'proj-src-cs': 'not a connection string at all' } });
t.call('renderConnLock', 'src');
check('an unparseable string still masks', shown(t.els['src-conn-locked']));
check('an unparseable string is not printed',
  !cardText(t.els).includes('not a connection string'));

// ── Saved-chip tooltip ───────────────────────────────────────────────────
// The chip is always visible, so its tooltip is part of the same surface.
const tip = t.sb.sconnPreview || vm.runInContext('sconnPreview', t.sb);
const csTip = tip({ mode: 'direct', connString: CS });
check('the chip tooltip shows the database name',
  csTip === 'Database: Conversion_DM', csTip);
check('the chip tooltip leaks no password, host or user',
  !/jY4mN7xP2w|86\.20\.94\.140|mtvne|1433/.test(csTip), csTip);
const fnTip = tip({ mode: 'azure', fnUrl: FN });
check('the chip tooltip does not expose the function host',
  fnTip === 'Azure Function', fnTip);

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
