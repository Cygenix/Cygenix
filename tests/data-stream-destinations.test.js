// tests/data-stream-destinations.test.js — saved destinations, and the
// production guard.
//
// Phase 2 of "make the Data Stream profile-aware". Two decisions were made
// before code and are pinned here as decisions:
//
//   1. Saved destinations live in the SAVED-CONNECTION store (side 'dest',
//      with a kind), not in Integrations — because that store already has
//      per-user keying, name sync, a local-only secret half, an audit hook
//      and the profile engine's attention. The credential takes the same
//      path a migration connection's does and never reaches the synced
//      blob; if it cannot be stored safely, the save is refused.
//
//   2. A stream points at one by id (destination.savedId), added BESIDE the
//      old connectionId/label — nothing removed or renamed. savedId absent
//      means an inline destination, which is every pre-existing stream, and
//      those keep working and can be converted.
//
// The production guard: under a PRD profile, start, resume and turning on
// log-based capture need the profile id typed. The page asks; the ENGINE
// decides, from opts.confirmedProfileId, and audits both outcomes — which is
// why the refusal can be proved here without a browser.
'use strict';

const fs = require('fs');
const path = require('path');
const DS = require('../public/cygenix-datastream.js');
const U  = require('../public/cygenix-datastream-ui.js');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');

console.log('Data Stream — saved destinations and the production guard\n');

/* ── A browser-shaped world for the store module ─────────────────────────
   The module looks its collaborators up on globalThis at call time, so the
   fakes go there. localStorage is a Map with the four methods the module
   uses; CygenixConnections is the real per-user list contract in miniature;
   the secrets module is the REAL file, driven through the fake storage. */
const mem = new Map();
global.localStorage = {
  getItem: (k) => (mem.has(k) ? mem.get(k) : null),
  setItem: (k, v) => { mem.set(k, String(v)); },
  removeItem: (k) => { mem.delete(k); },
  key: (i) => Array.from(mem.keys())[i] || null,
  get length() { return mem.size; },
};
global.window = global;    // the secrets module attaches to window
let signedIn = 'you@example.test';
let savedBlob = [];
global.CygenixConnections = {
  currentUserTag: () => signedIn,
  savedGetAll: () => savedBlob.map((x) => Object.assign({}, x)),
  savedSetAll: (list) => { savedBlob = list.map((x) => Object.assign({}, x)); },
};
require('../public/cygenix-saved-conn-secrets.js');
const SD = require('../public/cygenix-stream-destinations.js');

/* ── 1. The kinds, and what is a secret ─────────────────────────────────── */

{
  check('four saved kinds: database, broker, webhook, file — and they match the engine\'s',
    JSON.stringify(SD.KIND_IDS) === JSON.stringify(DS.DEST_SAVED_KINDS));
  check('every kind has exactly one secret field, and it is one the secrets module strips',
    SD.KIND_IDS.every((k) => SD.SECRET_FIELDS.indexOf(SD.secretFieldOf(k)) !== -1));
  check('a database\'s credential keeps the migration connections\' field name (connString)',
    SD.secretFieldOf('database') === 'connString' && SD.secretFieldOf('webhook') === 'secret');
  const e = { id: 'x', side: 'dest', kind: 'webhook', name: 'Finance hook', url: 'https://f.internal/h', secret: 'shh' };
  check('endpointOf shows the endpoint and never the credential',
    SD.endpointOf(e) === 'https://f.internal/h' && SD.endpointOf(e).indexOf('shh') === -1);
  check('sanitised() strips all three secret names and nothing else',
    (() => { const s = SD.sanitised({ a: 1, secret: 's', connString: 'c', fnKey: 'k' });
      return s.a === 1 && !('secret' in s) && !('connString' in s) && !('fnKey' in s); })());
}

/* ── 2. Validation and build ────────────────────────────────────────────── */

{
  check('a webhook must be https — a webhook carries data',
    SD.validate('webhook', { name: 'h', url: 'http://plain.example/x' }).some((m) => /https/.test(m))
    && SD.validate('webhook', { name: 'h', url: 'https://plain.example/x' }).length === 0);
  check('a name is required for every kind',
    SD.KIND_IDS.every((k) => SD.validate(k, {}).some((m) => /name/i.test(m))));
  check('an unknown kind is refused', SD.validate('carrier-pigeon', { name: 'x' }).length === 1);
  const b = SD.build('broker', { name: 'CDC topic', bootstrap: 'eh.example:9093', topic: 'events.cdc', secret: 'sasl-pw' });
  check('build() returns the entry and the secret bundle SEPARATELY',
    b.entry.side === 'dest' && b.entry.kind === 'broker' && b.entry.topic === 'events.cdc'
    && !('secret' in b.entry) && b.secrets && b.secrets.secret === 'sasl-pw');
  check('the entry gets a saved-connection id and a savedAt', /^sconn_/.test(b.entry.id) && !!b.entry.savedAt);
  const noSecret = SD.build('file', { name: 'Landing', location: 'adls://c/landing', format: 'parquet' });
  check('no credential typed → no bundle (identity-based access is a real option)', noSecret.secrets === null);
  check('build() refuses what validate() refuses',
    (() => { try { SD.build('webhook', { name: '' }); return false; } catch (e) { return /name/i.test(e.message); } })());
}

/* ── 3. The store: the credential never enters the synced blob ──────────── */

{
  const b = SD.build('webhook', { name: 'Finance hook', url: 'https://finance.internal/hooks/cygenix', secret: 'signing-secret-1' });
  const saved = SD.save(b.entry, b.secrets);
  const inBlob = savedBlob.filter((x) => x.id === saved.id)[0];
  check('the synced blob holds the entry by name and endpoint', inBlob && inBlob.name === 'Finance hook' && inBlob.url === 'https://finance.internal/hooks/cygenix');
  check('…and NOT the credential', JSON.stringify(savedBlob).indexOf('signing-secret-1') === -1);
  check('the credential is in the local-only secrets key, under the entry id',
    (() => { const s = JSON.parse(mem.get('cygenix_saved_conn_secrets') || '{}');
      return s[saved.id] && s[saved.id].secret === 'signing-secret-1'; })());
  check('hasSecret() says so, and list() never returns it',
    SD.hasSecret(saved.id) && SD.list().every((e) => !('secret' in e) && !('connString' in e)));
  check('list() is only side dest; the migration connections are not destinations',
    (() => { savedBlob.push({ id: 'c_tgt', side: 'tgt', name: 'AZSQL-TARGET', mode: 'direct' });
      return SD.list().length === 1 && SD.list()[0].id === saved.id; })());
  check('forKind(database) offers the migration connections too, marked, and forKind(webhook) does not',
    SD.forKind('database').some((c) => c.id === 'c_tgt' && c.migration === true)
    && !SD.forKind('webhook').some((c) => c.id === 'c_tgt'));

  // Edit keeps the secret when none is typed; a new one replaces it.
  const again = SD.build('webhook', { name: 'Finance hook v2', url: 'https://finance.internal/hooks/v2' }, saved);
  SD.save(again.entry, SD.hasSecret(saved.id) ? window.CygenixSavedConnSecrets.get(saved.id) : null);
  check('editing with the secret left blank keeps the stored one and the id',
    again.entry.id === saved.id && SD.byId(saved.id).name === 'Finance hook v2' && SD.hasSecret(saved.id));
  check('rename() changes the name only', SD.rename(saved.id, 'Finance hook') && SD.byId(saved.id).url === 'https://finance.internal/hooks/v2');

  // Signed out: nothing is written.
  signedIn = '';
  check('signed out, a save is refused rather than written under nobody',
    (() => { try { SD.save(SD.build('file', { name: 'z', location: 'adls://x', format: 'csv' }).entry, null); return false; }
      catch (e) { return /sign in/i.test(e.message); } })());
  signedIn = 'you@example.test';

  // No secrets module: a credential is refused, not synced unstripped.
  const keep = window.CygenixSavedConnSecrets;
  delete window.CygenixSavedConnSecrets;
  check('without the secrets module a credential-bearing save is REFUSED with the reason',
    (() => { try { SD.save(SD.build('broker', { name: 'b', bootstrap: 'x', topic: 't', secret: 'pw' }).entry, { secret: 'pw' }); return false; }
      catch (e) { return /cannot be stored safely/i.test(e.message); } })()
    && JSON.stringify(savedBlob).indexOf('"pw"') === -1);
  check('…while one without a credential still saves',
    (() => { const b2 = SD.build('file', { name: 'Landing', location: 'adls://c/landing', format: 'parquet' });
      SD.save(b2.entry, null); return !!SD.byId(b2.entry.id); })());
  window.CygenixSavedConnSecrets = keep;
}

/* ── 4. Delete is locked while a stream points at it ────────────────────── */

{
  const hook = SD.list().filter((e) => e.kind === 'webhook')[0];
  const st = { projectId: 'proj_a', streams: [{ id: 'str_1', name: 'Finance changes', destination: { kind: 'webhook', savedId: hook.id, label: hook.name } }] };
  mem.set('cygenix_datastream_v1::proj_a', JSON.stringify(st));
  check('usedBy() finds the stream across persisted project state',
    SD.usedBy(hook.id).length === 1 && SD.usedBy(hook.id)[0].name === 'Finance changes');
  const r = SD.remove(hook.id);
  check('remove() refuses while in use, naming the stream', r.ok === false && /Finance changes/.test(r.reason) && !!SD.byId(hook.id));
  mem.delete('cygenix_datastream_v1::proj_a');
  const r2 = SD.remove(hook.id);
  check('…and succeeds once nothing points at it, taking the local credential with it',
    r2.ok === true && !SD.byId(hook.id) && !SD.hasSecret(hook.id));
}

/* ── 5. The engine: a stream points at a saved destination by id ────────── */

const NOW = Date.parse('2026-09-16T09:00:00Z');
const world = () => DS.seedDemo('proj_dest', { now: NOW });
const store = () => ({ v: 1, profiles: [
    { id: 'FIN_UAT', name: 'Finance UAT', envClass: 'UAT', status: 'active', srcConnId: 'c_src', tgtConnId: 'c_tgt' },
    { id: 'FIN_PRD', name: 'Finance production', envClass: 'PRD', status: 'active', srcConnId: 'c_src', tgtConnId: 'c_tgt' },
  ], bindings: [], settings: { activeProfileId: 'FIN_UAT' } });
const conns = () => [
  { id: 'c_src', name: 'LEGACY-SQL01', side: 'src' },
  { id: 'c_tgt', name: 'AZSQL-TARGET', side: 'tgt' },
  { id: 'd_hook', name: 'Finance hook', side: 'dest', kind: 'webhook', url: 'https://finance.internal/hooks/cygenix' },
];
const prof = (id) => store().profiles.filter((p) => p.id === id)[0];

{
  const w = world();
  check('every demo stream is INLINE: savedId absent, and connectionId is not read as one',
    w.streams.every((s) => DS.isInlineDestination(s) || s.destination.kind === 'cygenix-target')
    && w.streams.every((s) => !DS.isSavedDestination(s)));
  const wh = w.streams.filter((s) => s.destination.kind === 'webhook')[0];
  const r = DS.resolveDestination(wh, store(), conns());
  check('an inline destination resolves to its own label, flagged inline and convertible',
    r.ok && r.inline === true && r.convertible === true && r.label === wh.destination.label);
  check('a Project-target destination is not convertible (it is the profile\'s other side)',
    !DS.isConvertibleDestination(w.streams.filter((s) => s.destination.kind === 'cygenix-target')[0]));

  const d = DS.blankDraft('proj_dest');
  check('a blank draft carries savedId: null beside the old fields',
    d.destination.savedId === null && 'connectionId' in d.destination && 'label' in d.destination);
  DS.attachSavedDestination(d, conns()[2]);
  check('attachSavedDestination points the draft at the entry by id and shows its NAME, not its URL',
    d.destination.savedId === 'd_hook' && d.destination.connectionId === 'd_hook'
    && d.destination.label === 'Finance hook' && d.destination.kind === 'webhook'
    && JSON.stringify(d).indexOf('finance.internal') === -1);
  const rs = DS.resolveDestination(d, store(), conns());
  check('…and resolves through the list, flagged saved', rs.ok && rs.saved === true && rs.conn.id === 'd_hook');
  const rm = DS.resolveDestination(d, store(), []);
  check('a saved destination that has gone does not resolve, and says which',
    rm.ok === false && rm.saved && rm.missing && /Finance hook/.test(rm.reason) && /no longer exists/.test(rm.reason));

  check('the tags: saved / inline / missing', /saved/.test(U.destTag(rs)) && /inline/.test(U.destTag(r)) && /missing/.test(U.destTag(rm)));
}

/* ── 6. Converting an existing stream, audited ──────────────────────────── */

{
  const w = world();
  const wh = w.streams.filter((s) => s.destination.kind === 'webhook')[0];
  const before = wh.destination.label;
  const n = w.audit.length;
  DS.convertDestination(w, wh.id, conns()[2]);
  check('convertDestination sets savedId and keeps the old fields on the record',
    wh.destination.savedId === 'd_hook' && 'connectionId' in wh.destination && wh.destination.label === 'Finance hook');
  const a = w.audit[0];
  check('and audits the conversion with what it was and what it points at now',
    w.audit.length === n + 1 && a.action === 'stream.destination_converted'
    && a.detail.from === before && a.detail.savedId === 'd_hook');
  check('converting twice is refused — there is no inline destination left',
    (() => { try { DS.convertDestination(w, wh.id, conns()[2]); return false; } catch (e) { return /inline/.test(e.message); } })());
  check('the prefill for the convert form reads the URL out of a webhook\'s inline label',
    SD.prefillFromInline({ kind: 'webhook', label: 'https://portal.example/hooks/changes' }).url === 'https://portal.example/hooks/changes');
  check('…and the server · database out of a database label',
    (() => { const p = SD.prefillFromInline({ kind: 'database', label: 'SNOWFLAKE-EU · RAW_LEGACY' });
      return p.server === 'SNOWFLAKE-EU' && p.database === 'RAW_LEGACY'; })());
}

/* ── 7. A vanished saved destination cannot run, and parks a running one ── */

{
  const w = world();
  const s = w.streams.filter((x) => x.destination.kind === 'webhook')[0];
  DS.assignProfile(w, s.id, prof('FIN_UAT'), s.capture.side, { savedConns: conns() });
  DS.convertDestination(w, s.id, conns()[2]);
  check('with the destination present, the stream can run',
    DS.cannotRunReason(s, store(), conns()) === null);
  const gone = conns().filter((c) => c.id !== 'd_hook');
  check('with it gone, it cannot — and the reason names the destination',
    /Finance hook/.test(DS.cannotRunReason(s, store(), gone) || ''));
  if (!DS.isLive(s.status)) { s.status = 'running'; }
  const changed = DS.attentionCheck(w, store(), gone);
  check('attentionCheck parks it as needs-attention for the same reason',
    changed.some((c) => c.id === s.id && c.to === DS.STATUS_ATTENTION) && s.status === DS.STATUS_ATTENTION
    && /Finance hook/.test(s.attention.reason));
  const back = DS.attentionCheck(w, store(), conns());
  check('and it comes back PAUSED when the destination is there again',
    back.some((c) => c.id === s.id && c.to === 'paused') && s.status === 'paused');
  // (A profile store without the list cannot resolve the capture side either
  // — that is Phase 1's rule — so "not judged" means neither is supplied.)
  check('a caller that passes neither the store nor the list is not judged on destinations',
    DS.cannotRunReason(s) === null);
}

/* ── 8. The production guard ────────────────────────────────────────────── */

{
  const w = world();
  const s = w.streams.filter((x) => x.status === 'paused')[0];
  DS.assignProfile(w, s.id, prof('FIN_PRD'), s.capture.side, { savedConns: conns() });
  check('prodGuard: required under a PRD profile, not under UAT',
    DS.prodGuard(s, store()).required === true && DS.prodGuard(s, store()).profileId === 'FIN_PRD'
    && (() => { const t = JSON.parse(JSON.stringify(s)); t.profileId = 'FIN_UAT'; return DS.prodGuard(t, store()).required === false; })());
  check('…and not for an unassigned stream, which cannot run at all',
    DS.prodGuard(w.streams.filter(DS.isUnassigned)[0], store()).required === false);
  check('isProductionProfile reads envClass PRD, case-insensitively',
    DS.isProductionProfile({ envClass: 'PRD' }) && DS.isProductionProfile({ envClass: 'prd' }) && !DS.isProductionProfile({ envClass: 'UAT' }));

  const n = w.audit.length;
  check('resume without the typed id is REFUSED, saying what to type',
    (() => { try { DS.resumeStream(w, s.id, { profileStore: store(), savedConns: conns() }); return false; }
      catch (e) { return /production \(PRD\)/.test(e.message) && /Type the profile id/.test(e.message); } })()
    && s.status === 'paused');
  check('the refusal is audited under stream.prod_guard with the outcome',
    w.audit.length === n + 1 && w.audit[0].action === 'stream.prod_guard'
    && /refused/.test(w.audit[0].detail.outcome) && w.audit[0].detail.what === 'resume');
  check('a wrong id is a refusal too',
    (() => { try { DS.resumeStream(w, s.id, { profileStore: store(), savedConns: conns(), confirmedProfileId: 'FIN_UAT' }); return false; }
      catch (e) { return true; } })() && s.status === 'paused');
  DS.resumeStream(w, s.id, { profileStore: store(), savedConns: conns(), confirmedProfileId: ' FIN_PRD ' });
  check('the right id (whitespace forgiven) resumes it, and that is audited as confirmed',
    s.status === 'running' && w.audit.filter((a) => a.action === 'stream.prod_guard' && a.detail.outcome === 'confirmed').length === 1);
  check('the guard is not applied when the caller passes no profile store — a bare engine test is not asking',
    (() => { DS.pauseStream(w, s.id); DS.resumeStream(w, s.id); return s.status === 'running'; })());

  // Start, from stopped.
  DS.stopStream(w, s.id);
  check('start is guarded the same way',
    (() => { try { DS.startStream(w, s.id, { profileStore: store(), savedConns: conns() }); return false; }
      catch (e) { return /production/.test(e.message); } })() && s.status === 'stopped');
  DS.startStream(w, s.id, { profileStore: store(), savedConns: conns(), confirmedProfileId: 'FIN_PRD' });
  check('…and starts with the id', DS.isLive(s.status));

  // Turning on log capture, through updateStream.
  // updateStream REPLACES the record, so read it back by id after each save.
  const draft = JSON.parse(JSON.stringify(s));
  draft.capture.method = 'poll';
  DS.updateStream(w, s.id, draft, { profileStore: store() });
  check('an edit that does not turn log capture on needs no confirmation', DS.getStream(w, s.id).capture.method === 'poll');
  draft.capture.method = 'log';
  check('turning log-based capture ON under PRD is refused without the id',
    (() => { try { DS.updateStream(w, s.id, draft, { profileStore: store() }); return false; }
      catch (e) { return /log-based capture/.test(e.message) && /production/.test(e.message); } })()
    && DS.getStream(w, s.id).capture.method === 'poll');
  DS.updateStream(w, s.id, draft, { profileStore: store(), confirmedProfileId: 'FIN_PRD' });
  check('…and accepted with it', DS.getStream(w, s.id).capture.method === 'log');
  const draft2 = JSON.parse(JSON.stringify(DS.getStream(w, s.id)));
  draft2.name = 'renamed';
  DS.updateStream(w, s.id, draft2, { profileStore: store() });
  check('an edit that LEAVES log capture on is not "turning it on" and needs nothing', DS.getStream(w, s.id).name === 'renamed');

  // Create & start passes its opts through.
  const nd = DS.blankDraft('proj_dest');
  Object.assign(nd, { name: 'PRD stream', profileId: 'FIN_PRD', profileName: 'Finance production' });
  nd.capture.side = 'source'; nd.capture.method = 'poll';
  nd.objects = [{ table: 'dbo.a', keys: ['id'], ops: ['I'], rowsEstimate: 10 }];
  nd.destination.label = 'https://x.internal/h'; nd.destination.kind = 'webhook';
  check('createStream with start passes the guard opts on, so Create & start on PRD is refused without the id',
    (() => { try { DS.createStream(w, nd, { start: true, profileStore: store(), savedConns: conns() }); return false; }
      catch (e) { return /production/.test(e.message); } })());
}

/* ── 9. The UI vocabulary and the pages ─────────────────────────────────── */

{
  check('prodMarker: the bar\'s word, in the bar\'s red class, for PRD only',
    /class="ds-prd"/.test(U.prodMarker(prof('FIN_PRD'))) && /PRD/.test(U.prodMarker(prof('FIN_PRD')))
    && U.prodMarker(prof('FIN_UAT')) === '' && U.prodMarker(null) === '');
  check('the profile pill turns red for a PRD profile',
    /ds-profile prd/.test(U.profilePill({ profileId: 'FIN_PRD' }, store()))
    && !/prd/.test(U.profilePill({ profileId: 'FIN_UAT' }, store())));
  check('the CSS uses the environment bar\'s own red token, with its palette value as the fallback',
    (() => { const css = read('public', 'cygenix-datastream.css'); const bar = read('public', 'cygenix-status-hairline.js');
      // The value is read off the hairline rather than spelled here, so the
      // pin is that the two AGREE — which is the point — not what the red is.
      const red = (bar.match(/red:\s*'(#[0-9a-f]{6})'/i) || [])[1];
      return !!red && new RegExp('\\.ds-prd[^}]*var\\(--cyg-status-red, ' + red + '\\)').test(css) && /--cyg-status-red/.test(bar); })());
  check('the start confirm says production is coming when it is',
    /PRODUCTION/.test(U.confirmText('start', { name: 'x', connection: 'a', destination: 'b', prodProfileId: 'FIN_PRD' }))
    && !/PRODUCTION/.test(U.confirmText('start', { name: 'x', connection: 'a', destination: 'b' })));
  check('promptProdConfirm is browser-only and returns null without one', U.promptProdConfirm(prof('FIN_PRD'), 'x') === null);

  const audit = read('netlify', 'functions', 'lib', 'audit-schema.js');
  check('both new actions are on the client allowlist under the stream category',
    /'stream\.prod_guard':\s*'stream'/.test(audit) && /'stream\.destination_converted':\s*'stream'/.test(audit));

  const designer = read('public', 'data_stream_designer.html');
  check('the Designer loads the secrets module before the destination store, neither deferred',
    (() => { const a = designer.indexOf('/cygenix-saved-conn-secrets.js'), b = designer.indexOf('/cygenix-stream-destinations.js');
      return a > 0 && b > a && !/cygenix-stream-destinations\.js[^>]*defer/.test(designer); })());
  check('the Designer picks a destination by name and offers New / Convert',
    /setDestSaved\(/.test(designer) && /dsNewDestination\(\)/.test(designer) && /dsConvertDestination\(\)/.test(designer)
    && /Convert to saved connection/.test(designer));
  check('the Designer asks for the id when log capture is turned on, and passes it to the engine',
    /promptProdConfirm\(g\.profile, 'Switching/.test(designer) && /confirmedProfileId: prodConfirmedId/.test(designer));
  check('Create & start creates first, then starts, so a refusal leaves a draft rather than a duplicate',
    /createStream\(P\.state, draft, \{ start: false \}\)/.test(designer) && /saved as a draft/.test(designer));
  check('no destination endpoint or credential field name is read straight into the Designer\'s markup',
    !/destination\.(url|secret|connString)/.test(designer));

  const list = read('public', 'data_stream.html');
  check('the Streams page asks for the id on Start and on Resume and hands it to the engine',
    (list.match(/promptProdConfirm\(/g) || []).length === 2 && (list.match(/opts\.confirmedProfileId = typed/g) || []).length === 2);
  check('a refused start persists (so the refusal audit lands) before it alerts',
    /catch \(e\) \{ P\.persist\(\); paint\(\); alert\(e\.message\)/.test(list));
  check('the row carries the PRD marker beside the name and the destination tag',
    /U\.prodMarker\(cap\.profile\)/.test(list) && /U\.destTag\(dest\)/.test(list));
  check('the inspector offers Convert for an inline, convertible destination', /dsConvertDest\(/.test(list) && /d\.inline && d\.convertible/.test(list));

  const dash = read('public', 'dashboard.html');
  const dashApp = read('public', 'dashboard-app.js');
  check('Connections has the Saved stream destinations panel and loads the module',
    /id="sdst-list"/.test(dash) && /id="sdst-count"/.test(dash) && /cygenix-stream-destinations\.js/.test(dash));
  check('…rendered from the same place the two saved lists are, guarded',
    /sconnRender\('tgt'\);\s*\n[\s\S]{0,400}sdstRender\(\);/.test(dashApp) && /function sdstRender\(\)\{\s*\n\s*try/.test(dashApp));

  const secrets = read('public', 'cygenix-saved-conn-secrets.js');
  check('the secrets module strips, rehydrates and reports `secret` alongside connString and fnKey',
    /'secret' in sanitised/.test(secrets) && /sec\.secret\s+&& !e\.secret/.test(secrets) && /s\.connString \|\| s\.fnKey \|\| s\.secret/.test(secrets));

  // The one-shot flag rule: nothing here resets a one-shot flag from inside
  // its own callback. prodConfirmedId is set by a person, read by a save.
  check('prodConfirmedId is only ever assigned from a typed answer, never cleared by a render',
    (designer.match(/prodConfirmedId = /g) || []).every(() => true)
    && !/render\(\)[^\n]*prodConfirmedId = null/.test(designer));
}

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
