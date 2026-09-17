/* ============================================================================
   cygenix-profile-apply.js — selecting a profile loads its connections
   ----------------------------------------------------------------------------
   Requested (Sep-2026). Selecting a connection profile changed the label in
   the top bar and the identity writes ran under, and nothing else: the LIVE
   source and target — the values the SQL Editor, Schema Explorer, Object
   Mapping, Data Import and the rest actually connect with — stayed whatever
   the Connections page last held. So a person who had defined FIN_3E_UAT as
   "Conversion → H Database 3" still had to open Connections and type both of
   those in by hand every time they switched. Twice a day, for people who
   switch environments twice a day.

   WHAT THIS DOES. On selection, the profile's two saved connections are
   copied into the live settings, through the one writer the live settings
   have (CygenixConnections.setActive), which is the write the sync layer
   already watches — so the change reaches Cosmos and the next machine the
   same way a hand-typed one does, as one save. On page load, the live
   settings are compared with what the selected profile would produce and
   re-applied if they have drifted. Both are COPIES: the saved entry is never
   edited, so a locked connection stays exactly as locked as it was.

   THE ONE DECISION THAT NEEDED CARE: a saved connection's credential is on
   the browser where it was typed and nowhere else (cygenix-saved-conn-
   secrets.js keeps it out of the synced list, deliberately). On any other
   browser the entry is a name and, for an Azure Function, a URL. This module
   loads what exists, names exactly which field is missing for which
   connection, and marks that side as "to finish" so the Connections page
   opens on it; when the person saves the value there, finishOnce() stores it
   into the entry's LOCAL secret store — a copy again — so that browser never
   asks for it a second time. It NEVER guesses: a live value that cannot be
   tied to the entry (a direct connection string, which is itself the secret)
   is left alone rather than silently adopted, and no field is ever blanked
   by a plan that lacks it.

   SHAPE, NOT MODE. A saved entry and the legacy save path can both file a
   plain connection string under the function-URL field when the mode toggle
   said "azure" (impGetConn's isHttpUrl guard exists because of it). The plan
   decides by the shape of the value: https:// is a Function URL, anything
   else is a connection string, whatever the stored mode claims.

   NO LOOPS, NO BURSTS. apply() writes local storage once; the sync layer's
   own 3-second debounce turns the profile write and the connections write
   into one save. The page-load check runs once per selection per session
   (a sessionStorage stamp) and only after the sync layer has finished its
   cloud load, so it cannot fight the load, and it never re-triggers itself:
   the applied event it dispatches is listened to by views, not by this file.
   A second apply within three seconds of the first for the same profile is
   dropped. Nothing here makes a network call of its own.

   GUARDRAILS ARE THE ENGINE'S. Draft and retired profiles are refused by
   cpSelectProfile as before. A PRD profile asks for its id typed, in the
   words the dashboard's tools use. UNKNOWN environments are untouched here
   and still blocked at write time by cpGuardWrite. Anything running blocks
   or asks first — see busy().

   Node-testable: every rule is a pure function of its arguments; the
   browser half (select, checkOnLoad, finishOnce, the notice) looks its
   collaborators up on the global at call time.
   ========================================================================== */
(function (root, factory) {
  var api = factory(root);
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixProfileApply) root.CygenixProfileApply = api;
})(typeof globalThis !== 'undefined' ? globalThis : (typeof window !== 'undefined' ? window : this), function (root) {
'use strict';

var SEEN_KEY   = 'cygenix_profile_apply_seen';   // sessionStorage: profileId::selectedAt, one check per selection
var FINISH_KEY = 'cygenix_profile_finish';       // sessionStorage: which side/entry the Connections page should open on
var MIN_APPLY_MS = 3000;
var STREAM_PREFIX = 'cygenix_datastream_v1::';
var FLAT_KEYS = ['cygenix_src_conn_string', 'cygenix_src_conn_mode', 'cygenix_src_fn_url', 'cygenix_src_fn_key',
  'cygenix_tgt_conn_string', 'cygenix_tgt_conn_mode', 'cygenix_fn_url', 'cygenix_fn_key',
  'cygenix_conn_string', 'cygenix_conn_mode'];
var FIELDS = ['srcConnString', 'srcConnMode', 'srcFnUrl', 'srcFnKey',
  'tgtConnString', 'tgtConnMode', 'tgtFnUrl', 'tgtFnKey'];

function isHttpUrl(v) { return /^https?:\/\//i.test(String(v || '').trim()); }
function str(v) { return String(v == null ? '' : v).trim(); }

/* ── The plan: pure ──────────────────────────────────────────────────── */

/* One side of a saved entry, as live fields, decided by the SHAPE of what it
   holds. `missing` names what this browser does not have. */
function normaliseEntry(entry) {
  var e = entry || {};
  var cs = str(e.connString), url = str(e.fnUrl), key = str(e.fnKey);
  var out = { mode: 'direct', connString: '', fnUrl: '', fnKey: '', missing: [] };
  if (isHttpUrl(url)) {
    out.mode = 'azure'; out.fnUrl = url; out.fnKey = key;
    // A function may genuinely need no key, so this is soft: reported, not
    // treated as "cannot connect".
    if (!key) out.missing.push({ field: 'fnKey', label: 'function key', hard: false });
    return out;
  }
  if (isHttpUrl(cs)) {
    out.mode = 'azure'; out.fnUrl = cs; out.fnKey = key;
    if (!key) out.missing.push({ field: 'fnKey', label: 'function key', hard: false });
    return out;
  }
  // Not a URL anywhere: a direct connection. The string may have been filed
  // under fnUrl by the old save path — take it from wherever it is.
  out.mode = 'direct';
  out.connString = cs || url;
  if (!out.connString) out.missing.push({ field: 'connString', label: 'connection string', hard: true });
  return out;
}

function profileOf(store, id) {
  var list = (store && store.profiles) || [];
  for (var i = 0; i < list.length; i++) if (list[i] && list[i].id === id) return list[i];
  return null;
}
function entryOf(savedConns, id) {
  var list = Array.isArray(savedConns) ? savedConns : [];
  for (var i = 0; i < list.length; i++) if (list[i] && list[i].id === id) return list[i];
  return null;
}

/* What selecting `profile` would put in the live settings. savedConns must
   be the REHYDRATED list (secrets merged in) — the browser half does that;
   a test passes what it wants. Never throws: refusals are in `reasons`. */
function plan(store, profileId, savedConns) {
  var p = profileOf(store, profileId);
  var out = { ok: false, profileId: profileId, profile: p, reasons: [], missing: [],
    src: null, tgt: null, fields: null };
  if (!p) { out.reasons.push('No profile ' + profileId + '.'); return out; }
  if (p.status !== 'active') { out.reasons.push('Profile ' + p.id + ' is ' + p.status + ' — only an active profile can be loaded.'); return out; }
  var src = entryOf(savedConns, p.srcConnId), tgt = entryOf(savedConns, p.tgtConnId);
  if (!src) out.reasons.push('The source connection of ' + p.id + ' (' + p.srcConnId + ') is not in the saved list.');
  if (!tgt) out.reasons.push('The target connection of ' + p.id + ' (' + p.tgtConnId + ') is not in the saved list.');
  if (out.reasons.length) return out;

  var s = normaliseEntry(src), t = normaliseEntry(tgt);
  out.src = { id: src.id, name: str(src.name) || src.id, mode: s.mode, missing: s.missing };
  out.tgt = { id: tgt.id, name: str(tgt.name) || tgt.id, mode: t.mode, missing: t.missing };
  s.missing.forEach(function (m) { out.missing.push({ side: 'src', connId: src.id, name: out.src.name, field: m.field, label: m.label, hard: m.hard }); });
  t.missing.forEach(function (m) { out.missing.push({ side: 'tgt', connId: tgt.id, name: out.tgt.name, field: m.field, label: m.label, hard: m.hard }); });
  out.fields = {
    srcConnString: s.connString, srcConnMode: s.mode, srcFnUrl: s.fnUrl, srcFnKey: s.fnKey,
    tgtConnString: t.connString, tgtConnMode: t.mode, tgtFnUrl: t.fnUrl, tgtFnKey: t.fnKey,
  };
  out.ok = true;
  return out;
}

/* A plan never blanks a field it does not have. For a missing function key
   the live key is kept when the live URL is the same URL — the key belongs
   to that endpoint. A missing connection string is NOT carried over: the
   string is the secret, nothing identifies it as this entry's, and copying
   the wrong database's credentials onto a profile is the failure this whole
   feature exists to prevent. */
function mergeWithLive(planned, live) {
  var f = Object.assign({}, planned || {});
  var l = live || {};
  ['src', 'tgt'].forEach(function (side) {
    var url = side + 'FnUrl', key = side + 'FnKey';
    if (f[url] && !f[key] && str(l[url]) === str(f[url]) && str(l[key])) f[key] = str(l[key]);
  });
  return f;
}

function sameFields(a, b) {
  var x = a || {}, y = b || {};
  for (var i = 0; i < FIELDS.length; i++) if (str(x[FIELDS[i]]) !== str(y[FIELDS[i]])) return false;
  return true;
}

/* The sentence the page shows. Names only — never a value. */
function summary(pl) {
  if (!pl || !pl.ok) return '';
  return 'Loaded ' + pl.profile.id + ': Source = ' + pl.src.name + ', Target = ' + pl.tgt.name;
}
function missingSentence(pl) {
  if (!pl || !pl.missing || !pl.missing.length) return '';
  return pl.missing.map(function (m) {
    return (m.side === 'src' ? 'Source' : 'Target') + ' "' + m.name + '": no ' + m.label + ' on this browser'
      + (m.hard ? ' — enter it once on Connections and it will be kept.' : '.');
  }).join(' ');
}

/* ── What is running: pure over the storage snapshot ───────────────────
   `snapshot` is { streams: [state, ...], jobs: [...], agentiveRun: bool }.
   Jobs the Task Agent is running BLOCK — a switch under a scheduled job is
   the exact "pointed at a different database mid-run" the brief names.
   A live stream and an agentive marker ASK: the stream resolves its own
   connection from the profile and the marker can be a day stale, so both
   are a question, not a wall. */
function busy(snapshot) {
  var s = snapshot || {};
  var block = [], confirm = [];
  (s.jobs || []).forEach(function (j) {
    var st = String((j && (j.executionStatus || j.status)) || '').toLowerCase();
    if (st === 'running' || st === 'in_progress' || st === 'queued') block.push('job "' + ((j && j.name) || j.id) + '" is ' + st);
  });
  (s.streams || []).forEach(function (st) {
    ((st && st.streams) || []).forEach(function (x) {
      var v = x && x.status;
      if (v === 'running' || v === 'lagging' || v === 'snapshotting') confirm.push('stream "' + x.name + '" is ' + v);
    });
  });
  if (s.agentiveRun) confirm.push('an agentive migration run may still be in progress');
  return { block: block, confirm: confirm, clear: !block.length && !confirm.length };
}

/* ── Browser half ──────────────────────────────────────────────────────── */
function conns() { return root && root.CygenixConnections; }
function profiles() { return root && root.CygenixProfiles; }
function secrets() { return root && root.CygenixSavedConnSecrets; }
function ls() { try { return root.localStorage; } catch (e) { return null; } }
function ss() { try { return root.sessionStorage; } catch (e) { return null; } }
function userName() {
  try { return (JSON.parse(ls().getItem('cygenix_user') || '{}').email) || ''; } catch (e) { return ''; }
}
/* The saved list with this browser's secrets merged in — the same read the
   Profiles page makes, so the two cannot disagree about what is present. */
function loadSavedConns() {
  var c = conns();
  if (!c || typeof c.savedGetAll !== 'function') return [];
  var list = [];
  try { list = c.savedGetAll() || []; } catch (e) { return []; }
  var m = secrets();
  if (m && typeof m.rehydrate === 'function') { try { m.rehydrate(list); } catch (e) { /* keep the names */ } }
  return list;
}
function snapshotBusy() {
  var L = ls();
  var out = { streams: [], jobs: [], agentiveRun: false };
  if (!L) return out;
  try {
    for (var i = 0; i < L.length; i++) {
      var k = L.key(i);
      if (k && k.indexOf(STREAM_PREFIX) === 0) { try { out.streams.push(JSON.parse(L.getItem(k) || 'null')); } catch (e) {} }
    }
    out.jobs = JSON.parse(L.getItem('cygenix_jobs') || '[]') || [];
    var ag = JSON.parse(L.getItem('cygenix_agentive_migration') || '{}') || {};
    var who = L.getItem('cygenix_active_user') || 'anon';
    var mark = ag[who];
    out.agentiveRun = !!(mark && mark.runId && (Date.now() - (mark.savedAt || 0)) < 24 * 60 * 60 * 1000);
  } catch (e) { /* an unreadable key is not a running job */ }
  return out;
}

var _lastApply = { profileId: null, at: 0 };

/* Write the plan into the live settings. Returns what was written, or null
   when nothing was (not signed in, or the same profile within 3 seconds). */
function apply(pl, opts) {
  var o = opts || {};
  if (!pl || !pl.ok) return null;
  var now = Date.now();
  if (_lastApply.profileId === pl.profile.id && (now - _lastApply.at) < MIN_APPLY_MS && !o.force) return null;
  var c = conns();
  if (!c || typeof c.setActive !== 'function') return null;
  var fields = mergeWithLive(pl.fields, typeof c.get === 'function' ? c.get() : {});
  if (!c.setActive(fields)) return null;
  _lastApply = { profileId: pl.profile.id, at: now };

  // The old flat keys are last-resort fallbacks in two readers; a stale one
  // must not shadow what was just written.
  var S = ss();
  if (S) FLAT_KEYS.forEach(function (k) { try { S.removeItem(k); } catch (e) {} });
  // Which side the Connections page should open on, and why.
  var hard = pl.missing.filter(function (m) { return m.hard; });
  if (S) {
    try {
      if (hard.length) S.setItem(FINISH_KEY, JSON.stringify({ profileId: pl.profile.id, items: pl.missing }));
      else S.removeItem(FINISH_KEY);
    } catch (e) {}
  }
  // A "target unreachable" mark from a test against the OLD database is
  // about a database nobody is pointed at any more.
  try { if (root.CygenixStatusHairline && root.CygenixStatusHairline.report) root.CygenixStatusHairline.report('db', null); } catch (e) {}
  try {
    root.dispatchEvent(new CustomEvent('cygenix:connections-applied', {
      detail: { profileId: pl.profile.id, src: pl.src.name, tgt: pl.tgt.name, missing: pl.missing, silent: !!o.silent },
    }));
  } catch (e) {}
  try {
    if (root.CygenixAudit && root.CygenixAudit.record) {
      root.CygenixAudit.record({
        action: 'profile.applied', category: 'connections',
        target: { type: 'profile', id: pl.profile.id, label: 'Profile: ' + pl.profile.id },
        summary: summary(pl) + (pl.missing.length ? ' (' + pl.missing.length + ' field(s) not on this browser)' : ''),
        detail: { src: pl.src.name, tgt: pl.tgt.name, srcMode: pl.src.mode, tgtMode: pl.tgt.mode,
          missing: pl.missing.map(function (m) { return m.side + ':' + m.field; }), silent: !!o.silent },
      });
    }
  } catch (e) {}
  return fields;
}

/* The typed confirmation, in the dashboard's words. Returns true to go on. */
function prodConfirm(profile, ask) {
  var fn = ask || (typeof root.prompt === 'function' ? root.prompt.bind(root) : null);
  if (!fn) return false;
  var typed = fn('This is a PRD profile (' + profile.id + ').\nSelecting it loads its production source and target '
    + 'into the live connections for every tool.\n\nType the profile id to continue:');
  if (typed == null) return false;
  if (String(typed).trim() !== profile.id) {
    if (typeof root.alert === 'function') root.alert('Confirmation did not match ' + profile.id + ' — nothing was changed.');
    return false;
  }
  return true;
}

/* The whole act, from the Profiles page: check, confirm, select, save, load.
   Returns { ok, message, plan, reason }. Never throws to the page. */
function select(profileId, opts) {
  var o = opts || {};
  var P = profiles();
  if (!P) return { ok: false, reason: 'Profiles module is not loaded.' };
  var store = P.cpLoad();
  var p = profileOf(store, profileId);
  if (!p) return { ok: false, reason: 'No profile ' + profileId + '.' };
  if (p.status !== 'active') return { ok: false, reason: 'Profile ' + p.id + ' is ' + p.status + ' — only an active profile can be selected.' };

  var b = busy(snapshotBusy());
  if (b.block.length) {
    return { ok: false, reason: 'Cannot switch while ' + b.block.join('; ') + '. Wait for it to finish, or stop it first.' };
  }
  if (b.confirm.length) {
    var ask = o.confirm || (typeof root.confirm === 'function' ? root.confirm.bind(root) : null);
    if (!ask || !ask('Switching profile while ' + b.confirm.join('; ') + '.\n\nContinue anyway?')) {
      return { ok: false, reason: 'Switch cancelled — ' + b.confirm.join('; ') + '.' };
    }
  }
  if (String(p.envClass || '').toUpperCase() === 'PRD' && !prodConfirm(p, o.prompt)) {
    P.cpEvent(store, { type: 'profile.select_refused', profileId: p.id, why: 'typed confirmation', by: userName() }, Date.now());
    P.cpSave(store);
    return { ok: false, reason: 'Selecting ' + p.id + ' needs its id typed to confirm.', refused: true };
  }

  try { P.cpSelectProfile(store, p.id, userName(), Date.now()); }
  catch (e) { return { ok: false, reason: String(e.message || e) }; }
  var pl = plan(store, p.id, loadSavedConns());
  if (pl.ok) {
    P.cpEvent(store, { type: 'profile.applied', profileId: p.id, src: pl.src.name, tgt: pl.tgt.name,
      missing: pl.missing.map(function (m) { return m.side + ':' + m.field; }), by: userName() }, Date.now());
  }
  P.cpSave(store);
  var written = pl.ok ? apply(pl, { force: true }) : null;
  try { root.dispatchEvent(new CustomEvent('cygenix:profiles-changed')); } catch (e) {}
  if (!pl.ok) {
    return { ok: true, selected: true, applied: false, plan: pl,
      message: 'Selected ' + p.id + ', but its connections could not be loaded: ' + pl.reasons.join(' ') };
  }
  return { ok: true, selected: true, applied: !!written, plan: pl,
    message: summary(pl), missing: missingSentence(pl) };
}

/* Does the live store already hold what the selected profile would give it?
   Pure over its arguments so a test can ask without a browser. */
function drift(store, savedConns, live) {
  var id = store && store.settings && store.settings.activeProfileId;
  if (!id) return { profileId: null, drifted: false };
  var pl = plan(store, id, savedConns);
  if (!pl.ok) return { profileId: id, drifted: false, plan: pl };
  var want = mergeWithLive(pl.fields, live);
  return { profileId: id, plan: pl, want: want, drifted: !sameFields(want, live),
    hard: pl.missing.filter(function (m) { return m.hard; }) };
}

/* On page load: once per selection per session, after the cloud load. */
function checkOnLoad() {
  var P = profiles(), c = conns(), S = ss();
  if (!P || !c || !S) return null;
  var store = P.cpLoad();
  var st = store && store.settings;
  if (!st || !st.activeProfileId) return null;
  var stamp = st.activeProfileId + '::' + (st.selectedAt || 0);
  try { if (S.getItem(SEEN_KEY) === stamp) return null; S.setItem(SEEN_KEY, stamp); } catch (e) {}
  var d = drift(store, loadSavedConns(), c.get());
  if (!d.drifted) return d;
  if (!d.hard.length) {
    // force: the once-per-selection stamp above is this path's own guard;
    // the 3-second guard is for a burst of selects, not for a load check
    // that follows a select on the same page.
    apply(d.plan, { silent: true, force: true });
    return Object.assign({ applied: true }, d);
  }
  // Something this browser cannot supply: say so once, load nothing over
  // the live values, and point at the field.
  try { S.setItem(FINISH_KEY, JSON.stringify({ profileId: d.profileId, items: d.plan.missing })); } catch (e) {}
  notice('Profile ' + d.profileId + ': ' + missingSentence(d.plan), d.plan.missing[0]);
  return Object.assign({ applied: false, warned: true }, d);
}

/* What the Connections page should open on, if anything. */
function pendingFinish() {
  var S = ss(); if (!S) return null;
  try { return JSON.parse(S.getItem(FINISH_KEY) || 'null'); } catch (e) { return null; }
}
function clearFinish() { var S = ss(); if (S) { try { S.removeItem(FINISH_KEY); } catch (e) {} } }

/* Called by the Connections page after a Save. For each side the selected
   profile points at whose entry has no credential on this browser, the value
   just saved is stored into that entry's LOCAL secret store — a copy, the
   synced entry untouched — so it is never asked for again here. Only when
   the side was marked "to finish" by a load, or, for an Azure Function, when
   the URL saved is the entry's URL: a hand-typed string with nothing tying it
   to the entry is not adopted. Returns the names finished. */
function finishOnce(savedFields) {
  var P = profiles(), m = secrets();
  if (!P || !m || typeof m.set !== 'function') return [];
  var store = P.cpLoad();
  var id = store && store.settings && store.settings.activeProfileId;
  if (!id) return [];
  var p = profileOf(store, id); if (!p) return [];
  var list = loadSavedConns();
  var pending = pendingFinish();
  var marked = {};
  ((pending && pending.profileId === id && pending.items) || []).forEach(function (it) { marked[it.side] = true; });
  var done = [];
  var f = savedFields || {};
  [['src', p.srcConnId], ['tgt', p.tgtConnId]].forEach(function (pair) {
    var side = pair[0], entry = entryOf(list, pair[1]);
    if (!entry || m.hasSecret(entry.id)) return;
    var n = normaliseEntry(entry);
    var bundle = null;
    if (n.mode === 'azure') {
      if (str(f[side + 'FnUrl']) === n.fnUrl && str(f[side + 'FnKey'])) bundle = { fnKey: str(f[side + 'FnKey']) };
    } else if (marked[side] && str(f[side + 'ConnString'])) {
      bundle = { connString: str(f[side + 'ConnString']) };
    }
    if (!bundle) return;
    m.set(entry.id, bundle);
    done.push({ side: side, name: str(entry.name) || entry.id });
  });
  if (done.length) {
    clearFinish();
    try {
      P.cpEvent(store, { type: 'profile.credential_kept', profileId: id,
        names: done.map(function (d) { return d.name; }), by: userName() }, Date.now());
      P.cpSave(store);
    } catch (e) {}
  }
  return done;
}

/* A small dismissible bar, on any page. Words only; no value ever reaches it. */
function notice(text, first) {
  var doc = root && root.document;
  if (!doc || !doc.body) return;
  var old = doc.getElementById('cyg-profile-notice');
  if (old) old.parentNode.removeChild(old);
  var el = doc.createElement('div');
  el.id = 'cyg-profile-notice';
  el.setAttribute('role', 'status');
  el.style.cssText = 'position:fixed;left:50%;transform:translateX(-50%);bottom:18px;z-index:2400;max-width:min(720px,94vw);'
    + 'background:var(--bg2,#fff);color:var(--text,#111);border:0.5px solid var(--amber,#B26A00);border-left:4px solid var(--amber,#B26A00);'
    + 'border-radius:10px;padding:10px 12px;font:12.5px/1.5 var(--serif,system-ui,sans-serif);box-shadow:0 12px 32px rgba(0,0,0,0.25);'
    + 'display:flex;gap:10px;align-items:center;flex-wrap:wrap';
  var span = doc.createElement('span'); span.textContent = text; el.appendChild(span);
  var a = doc.createElement('a');
  a.href = '/dashboard#goto=connections'; a.textContent = 'Finish it on Connections';
  a.style.cssText = 'color:var(--accent,#4A5BD6);font-weight:600;white-space:nowrap';
  el.appendChild(a);
  var x = doc.createElement('button');
  x.type = 'button'; x.textContent = 'Dismiss'; x.setAttribute('aria-label', 'Dismiss');
  x.style.cssText = 'margin-left:auto;background:none;border:0.5px solid var(--border2,#ccc);border-radius:6px;padding:3px 8px;cursor:pointer;color:inherit;font:inherit';
  x.onclick = function () { if (el.parentNode) el.parentNode.removeChild(el); };
  el.appendChild(x);
  doc.body.appendChild(el);
}

/* Boot: run the load-time check ONCE, after the sync layer has loaded the
   cloud copy (so we compare against what this session will actually use),
   or after a short wait on a page without the sync layer. Both paths set
   `ran` first, so a late event cannot run it twice. */
function boot() {
  if (!root || !root.addEventListener) return;
  var ran = false;
  var run = function () { if (ran) return; ran = true; try { checkOnLoad(); } catch (e) { /* the page must load regardless */ } };
  var S = root.CygenixSync;
  var loadedNow = function () {
    try { return !!(S && typeof S.getHealth === 'function' && S.getHealth().lastLoadAt); } catch (e) { return false; }
  };
  if (loadedNow()) { setTimeout(run, 0); return; }
  // Two signals, because the sync layer sends two: 'cygenix-sync-loaded'
  // when a cloud copy was applied, and only a health change when the cloud
  // was verified EMPTY (a first sign-in). Either is "the load is over".
  root.addEventListener('cygenix-sync-loaded', run, { once: true });
  root.addEventListener('cygenix-sync-health', function onHealth() {
    if (!loadedNow()) return;
    root.removeEventListener('cygenix-sync-health', onHealth);
    run();
  });
  setTimeout(run, S ? 12000 : 1500);      // the sync's own timeout is shorter than this
}
if (root && root.document) {
  if (root.document.readyState === 'loading') root.document.addEventListener('DOMContentLoaded', boot);
  else boot();
}

return {
  SEEN_KEY: SEEN_KEY, FINISH_KEY: FINISH_KEY, FIELDS: FIELDS, MIN_APPLY_MS: MIN_APPLY_MS,
  isHttpUrl: isHttpUrl, normaliseEntry: normaliseEntry, plan: plan, mergeWithLive: mergeWithLive,
  sameFields: sameFields, summary: summary, missingSentence: missingSentence, busy: busy, drift: drift,
  loadSavedConns: loadSavedConns, snapshotBusy: snapshotBusy,
  apply: apply, select: select, checkOnLoad: checkOnLoad, finishOnce: finishOnce,
  pendingFinish: pendingFinish, clearFinish: clearFinish, notice: notice,
};
});
