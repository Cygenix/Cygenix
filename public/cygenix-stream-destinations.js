/* ══════════════════════════════════════════════════════════════════════════
   cygenix-stream-destinations.js — saved destinations for the Data Stream
   ══════════════════════════════════════════════════════════════════════════
   Phase 2 of "make the Data Stream profile-aware" (Sep-2026).

   WHAT A SAVED DESTINATION IS. An entry in the same per-user saved-connection
   store the migration connections live in (`cygenix_saved_connections`, via
   CygenixConnections.savedGetAll/savedSetAll) with:

     side: 'dest'                            never 'src' or 'tgt'
     kind: 'database' | 'broker' | 'webhook' | 'file'
     name                                    what a stream shows
     ...the endpoint fields for its kind     url / topic / location / server
     secret  (database: connString)          THE CREDENTIAL — see below

   A stream then points at it by id (destination.savedId) exactly as it
   points at its profile, and the engine looks the entry up when the stream
   runs (CygenixDataStream.resolveDestination). Change the webhook's URL here
   and every stream delivering to it follows, with no edit to the stream.

   WHY THIS STORE AND NOT INTEGRATIONS. The store already has everything a
   central credential home needs and the alternative has none of it:
     * per-user keying inside the blob;
     * cloud sync of the NAMES and endpoints, so a destination made on one
       machine is there on the next;
     * a local-only secret half — cygenix-saved-conn-secrets.js strips
       `connString`, `fnKey` and (now) `secret` out of every entry before
       the synced blob is written, and keeps them in a key the sync layer
       does not watch. A credential never reaches Cosmos. On a new browser
       the entry arrives by name and the credential has to be re-entered;
       that is the trade-off the migration connections already accepted;
     * an audit hook on every write (connection.create/edit/delete);
     * it is the list the profile engine already reads.
   Integrations (`cygenix_integrations`) holds ONE configuration per
   connector, not a list of named endpoints, and its whole blob is
   browser-local. It is the wrong shape for "five webhooks, by name".

   THE SECRET NEVER TAKES THE UNSANITISED PATH. Every write here goes through
   split() before savedSetAll. If the secrets module is not on the page and
   the entry carries a credential, the save is REFUSED with the reason —
   not written unstripped with a console warning, which is what the
   dashboard's older sconnSetAll does. A destination without a credential
   (a webhook with no signing secret, a landing zone reached by identity)
   saves fine either way.

   DELETE IS GUARDED. A destination a stream still points at cannot be
   deleted; the stream would be parked as needs-attention the next tick.
   usedBy() scans every project's stream state in this browser for
   destination.savedId. It cannot see another browser's streams — that is
   the same limit the stream module has always had, and it is why the
   engine treats a missing saved destination as needs-attention rather than
   as impossible.

   NODE-TESTABLE. Nothing in the store half touches the DOM. The browser
   globals it needs (CygenixConnections, CygenixSavedConnSecrets,
   localStorage) are looked up at call time on `root`, so a test can supply
   fakes on globalThis. The editor half (openEditor, renderChips) is DOM and
   is simply absent without a document.
   ══════════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  var api = factory(root);
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixStreamDestinations) root.CygenixStreamDestinations = api;
})(typeof globalThis !== 'undefined' ? globalThis : (typeof window !== 'undefined' ? window : this), function (root) {
'use strict';

var SIDE = 'dest';
var STATE_PREFIX = 'cygenix_datastream_v1::';

/* ── The kinds ─────────────────────────────────────────────────────────────
   One secret field per kind, always called `secret` except for a database,
   whose credential is its connection string and keeps the field name the
   migration connections use — so the secrets module strips it under the
   name it already knows. Every other field is an endpoint, not a credential,
   and is safe in the synced blob. */
var KINDS = {
  database: {
    label: 'Database / warehouse',
    blurb: 'Another database — a warehouse, a reporting copy, a client platform.',
    fields: [
      { key: 'dialect',  label: 'Engine', type: 'select', options: ['mssql', 'postgres', 'snowflake', 'mysql', 'other'], required: true },
      { key: 'server',   label: 'Server / account', placeholder: 'warehouse.eu.example.net', required: true },
      { key: 'database', label: 'Database', placeholder: 'RAW_LEGACY' },
    ],
    secret: { key: 'connString', label: 'Connection string (stays on this browser)',
      placeholder: 'Server=…;Database=…;User Id=…;Password=…' },
    endpoint: function (e) { return [e.server, e.database].filter(Boolean).join(' · '); },
  },
  broker: {
    label: 'Broker topic',
    blurb: 'A Kafka or Event Hub topic other systems subscribe to.',
    fields: [
      { key: 'bootstrap', label: 'Bootstrap servers / namespace', placeholder: 'events.servicebus.windows.net:9093', required: true },
      { key: 'topic',     label: 'Topic', placeholder: 'events.legacy.cdc', required: true },
    ],
    secret: { key: 'secret', label: 'SASL password or connection string (stays on this browser)', placeholder: 'optional' },
    endpoint: function (e) { return [e.bootstrap, e.topic].filter(Boolean).join(' › '); },
  },
  webhook: {
    label: 'Webhook',
    blurb: 'An HTTPS endpoint Cygenix posts each batch to.',
    fields: [
      { key: 'url', label: 'URL', placeholder: 'https://finance.internal/hooks/cygenix', required: true, validate: 'https' },
    ],
    secret: { key: 'secret', label: 'Signing secret, sent as X-Cygenix-Signature (stays on this browser)', placeholder: 'optional' },
    endpoint: function (e) { return e.url || ''; },
  },
  file: {
    label: 'File landing zone',
    blurb: 'A landing zone — files dropped for something else to pick up.',
    fields: [
      { key: 'location', label: 'Location', placeholder: 'adls://cygenix/landing/target', required: true },
      { key: 'format',   label: 'Format', type: 'select', options: ['parquet', 'csv', 'json'], required: true },
    ],
    secret: { key: 'secret', label: 'SAS token or access key (stays on this browser)', placeholder: 'optional — leave empty to use identity' },
    endpoint: function (e) { return [e.location, e.format ? '(' + e.format + ')' : ''].filter(Boolean).join(' '); },
  },
};
var KIND_IDS = Object.keys(KINDS);

/* Three field names, and nothing else, ever hold a credential. Every
   display path below is built by NAMING what to show, but this list is the
   second line: anything reaching a title or a chip is passed through
   sanitised() first. */
var SECRET_FIELDS = ['secret', 'connString', 'fnKey'];

function sanitised(entry) {
  var out = Object.assign({}, entry || {});
  SECRET_FIELDS.forEach(function (k) { delete out[k]; });
  return out;
}
function secretFieldOf(kind) { return (KINDS[kind] && KINDS[kind].secret.key) || 'secret'; }
function isDestination(e) { return !!e && e.side === SIDE && KIND_IDS.indexOf(e.kind) !== -1; }
function kindLabel(kind) { return (KINDS[kind] && KINDS[kind].label) || String(kind || ''); }
/* The endpoint as words. Never a credential: built from named fields. */
function endpointOf(entry) {
  var e = sanitised(entry);
  var k = KINDS[e.kind];
  return k ? k.endpoint(e) : '';
}

/* ── Building and validating ─────────────────────────────────────────── */
function validate(kind, values) {
  var errors = [];
  var k = KINDS[kind];
  var v = values || {};
  if (!k) { errors.push('Pick a destination kind.'); return errors; }
  if (!String(v.name || '').trim()) errors.push('Give the destination a name.');
  k.fields.forEach(function (f) {
    var val = String(v[f.key] == null ? '' : v[f.key]).trim();
    if (f.required && !val) errors.push(f.label + ' is required.');
    if (f.validate === 'https' && val && !/^https:\/\/[^\s]+$/i.test(val)) {
      errors.push(f.label + ' must start with https:// — a webhook carries data, and it does not go in the clear.');
    }
    if (f.type === 'select' && val && f.options.indexOf(val) === -1) errors.push(f.label + ' must be one of ' + f.options.join(', ') + '.');
  });
  return errors;
}

/* Build the entry the store will hold. Returns the sanitised entry and the
   secret bundle SEPARATELY, so the caller cannot write one without deciding
   what to do with the other. `existing` keeps the id (and savedAt) on edit. */
function build(kind, values, existing) {
  var errors = validate(kind, values);
  if (errors.length) throw new Error(errors.join(' '));
  var k = KINDS[kind];
  var v = values || {};
  var entry = {
    id: (existing && existing.id) || ('sconn_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7)),
    side: SIDE, kind: kind,
    name: String(v.name).trim().slice(0, 60),
    savedAt: (existing && existing.savedAt) || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
  k.fields.forEach(function (f) { entry[f.key] = String(v[f.key] == null ? '' : v[f.key]).trim(); });
  var secretVal = String(v[k.secret.key] == null ? '' : v[k.secret.key]).trim();
  var secrets = null;
  if (secretVal) { secrets = {}; secrets[k.secret.key] = secretVal; }
  return { entry: entry, secrets: secrets };
}

/* Prefill for "Convert to saved connection": the inline label is the only
   thing the stream knows, so it becomes the name and, where it looks like
   the endpoint (a URL, a location, a topic), the endpoint too. */
function prefillFromInline(destination) {
  var d = destination || {};
  var label = String(d.label || '').trim();
  var v = { name: label };
  if (d.kind === 'webhook' && /^https?:\/\//i.test(label)) v.url = label;
  if (d.kind === 'file' && /^[a-z0-9+.-]+:\/\//i.test(label)) v.location = label;
  if (d.kind === 'broker') {
    var parts = label.split(/\s*[›>]\s*/);
    if (parts.length === 2) { v.bootstrap = parts[0]; v.topic = parts[1]; } else v.topic = label;
  }
  if (d.kind === 'database') {
    var sp = label.split(/\s*·\s*/);
    v.server = sp[0] || ''; v.database = sp[1] || '';
  }
  return v;
}

/* ── The store ─────────────────────────────────────────────────────────── */
function conns() { return root && root.CygenixConnections; }
function secretsModule() { return root && root.CygenixSavedConnSecrets; }

function listAll() {
  var c = conns();
  if (!c || typeof c.savedGetAll !== 'function') return [];
  try { return c.savedGetAll() || []; } catch (e) { return []; }
}
/* Every saved destination, credentials stripped. */
function list() { return listAll().filter(isDestination).map(sanitised); }
function byId(id) { return list().filter(function (e) { return e.id === id; })[0] || null; }
/* The choices for one kind. For a database the migration connections
   (side src/tgt) are offered too, marked, because a reporting copy is very
   often one of them. */
function forKind(kind) {
  var all = listAll();
  var out = all.filter(function (e) { return isDestination(e) && e.kind === kind; }).map(sanitised);
  if (kind === 'database') {
    all.filter(function (e) { return e && (e.side === 'src' || e.side === 'tgt'); }).forEach(function (e) {
      var s = sanitised(e); s.migration = true; out.push(s);
    });
  }
  return out;
}
function hasSecret(id) {
  var m = secretsModule();
  return !!(m && typeof m.hasSecret === 'function' && m.hasSecret(id));
}

/* Save (add or replace). The sanitised entry goes to the synced list; the
   secret bundle goes to the local-only store — or, if there is a bundle and
   no local store to put it in, nothing is written and the caller is told. */
function save(entry, secrets) {
  var c = conns();
  if (!c || typeof c.savedGetAll !== 'function') throw new Error('Saved connections are not available on this page.');
  if (!c.currentUserTag || !c.currentUserTag()) throw new Error('Sign in to save a destination.');
  var m = secretsModule();
  if (secrets && !(m && typeof m.set === 'function')) {
    throw new Error('The credential cannot be stored safely on this page (secrets module missing), so nothing was saved. '
      + 'Save the destination without a credential, or add it from Connections.');
  }
  var clean = sanitised(entry);
  var all = c.savedGetAll() || [];
  var i = -1;
  for (var n = 0; n < all.length; n++) if (all[n] && all[n].id === clean.id) { i = n; break; }
  if (i >= 0) all[i] = clean; else all.push(clean);
  if (all.length > 50) throw new Error('The saved-connection list is full (50). Delete one first.');
  c.savedSetAll(all);
  if (m && typeof m.set === 'function') m.set(clean.id, secrets || null);
  return clean;
}
function rename(id, name) {
  var e = byId(id);
  if (!e) return false;
  var nm = String(name || '').trim().slice(0, 60);
  if (!nm) return false;
  var c = conns();
  var all = c.savedGetAll() || [];
  for (var n = 0; n < all.length; n++) if (all[n] && all[n].id === id) { all[n] = Object.assign({}, sanitised(all[n]), { name: nm }); }
  c.savedSetAll(all);
  return true;
}

/* Which streams, in any project in this browser, point at this destination.
   Reads the persisted state directly rather than the live engine, because
   the Connections page has no stream engine loaded. */
function usedBy(id) {
  var out = [];
  var ls = root && root.localStorage;
  if (!ls || !id) return out;
  try {
    for (var i = 0; i < ls.length; i++) {
      var key = ls.key(i);
      if (!key || key.indexOf(STATE_PREFIX) !== 0) continue;
      var st = JSON.parse(ls.getItem(key) || 'null');
      ((st && st.streams) || []).forEach(function (s) {
        if (s && s.destination && s.destination.savedId === id) {
          out.push({ id: s.id, name: s.name, projectId: st.projectId || key.slice(STATE_PREFIX.length) });
        }
      });
    }
  } catch (e) { /* unreadable state is not a use */ }
  return out;
}
function remove(id) {
  var e = byId(id);
  if (!e) return { ok: false, reason: 'No such destination.' };
  var users = usedBy(id);
  if (users.length) {
    return { ok: false, reason: '"' + e.name + '" is used by ' + users.length + ' stream'
      + (users.length === 1 ? '' : 's') + ' (' + users.map(function (u) { return u.name; }).join(', ')
      + '). Point them elsewhere first.' , usedBy: users };
  }
  var c = conns();
  var all = (c.savedGetAll() || []).filter(function (x) { return !(x && x.id === id); });
  c.savedSetAll(all);
  var m = secretsModule();
  if (m && typeof m.set === 'function') m.set(id, null);
  return { ok: true };
}

/* ══════════════════════════════════════════════════════════════════════════
   The editor — browser only
   ══════════════════════════════════════════════════════════════════════════
   One dialog, used by the Designer's step 3 and by Connections. It injects
   its own small stylesheet because the two pages share no CSS, and it never
   echoes a stored credential back into the form: on edit the secret field is
   blank with a "kept" note, and leaving it blank KEEPS the stored one. */
var CSS = ''
  + '.sdst-modal{position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:2100;display:flex;align-items:center;justify-content:center;padding:1rem}'
  + '.sdst-card{background:var(--bg2,#fff);border:0.5px solid var(--border2,#ccc);border-radius:14px;width:min(520px,96vw);max-height:88vh;display:flex;flex-direction:column;box-shadow:0 24px 60px rgba(0,0,0,0.5);color:var(--text,#111);font-family:var(--serif,inherit)}'
  + '.sdst-head{padding:0.85rem 1rem;border-bottom:0.5px solid var(--border,#ddd);font-size:13.5px;font-weight:600}'
  + '.sdst-body{padding:0.85rem 1rem;overflow:auto;font-size:12px;line-height:1.6;color:var(--text2,#333)}'
  + '.sdst-foot{padding:0.7rem 1rem;border-top:0.5px solid var(--border,#ddd);display:flex;gap:0.45rem;justify-content:flex-end;flex-wrap:wrap}'
  + '.sdst-field{margin-bottom:0.7rem}.sdst-field label{display:block;font-size:10px;letter-spacing:0.06em;color:var(--text3,#666);margin-bottom:0.25rem}'
  + '.sdst-field input,.sdst-field select{width:100%;box-sizing:border-box;font:inherit;font-size:12px;padding:6px 8px;border:0.5px solid var(--border2,#ccc);border-radius:6px;background:var(--bg3,#fafafa);color:var(--text,#111)}'
  + '.sdst-hint{font-size:10.5px;color:var(--text3,#666);margin-top:0.2rem}'
  + '.sdst-err{color:var(--red,#C0392B);font-size:11.5px;margin-bottom:0.6rem;white-space:pre-line}'
  + '.sdst-btn{font:inherit;font-size:12px;padding:6px 12px;border-radius:6px;border:0.5px solid var(--border2,#ccc);background:var(--bg3,#fafafa);color:var(--text,#111);cursor:pointer}'
  + '.sdst-btn.primary{background:var(--accent,#4A5BD6);border-color:var(--accent,#4A5BD6);color:#fff}'
  + '.sdst-chip{display:inline-flex;align-items:center;gap:0.4rem;background:var(--bg3,#f5f5f5);border:0.5px solid var(--border2,#ccc);border-radius:100px;padding:3px 4px 3px 10px;font-size:11px;color:var(--text2,#333)}'
  + '.sdst-chip .k{font-family:var(--mono,monospace);font-size:9px;opacity:0.6}'
  + '.sdst-chip .n{cursor:pointer}.sdst-chip button{background:none;border:none;color:var(--text3,#666);cursor:pointer;padding:2px 4px;font-size:11px;line-height:1;border-radius:3px}'
  + '.sdst-chip button:hover{background:var(--bg4,#eee);color:var(--text,#111)}.sdst-chip button.danger:hover{color:var(--red,#C0392B)}'
  + '.sdst-empty{font-size:11px;color:var(--text3,#666);font-style:italic}';

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
    return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
  });
}
function ensureCss(doc) {
  if (doc.getElementById('sdst-css')) return;
  var st = doc.createElement('style'); st.id = 'sdst-css'; st.textContent = CSS;
  doc.head.appendChild(st);
}

/* openEditor({ kind, entry, prefill, lockKind, onSaved }) */
function openEditor(opts) {
  var o = opts || {};
  var doc = root && root.document;
  if (!doc) return null;
  ensureCss(doc);
  var existing = o.entry ? sanitised(o.entry) : null;
  var kind = (existing && existing.kind) || o.kind || 'webhook';
  var values = Object.assign({}, o.prefill || {}, existing || {});

  var wrap = doc.createElement('div');
  wrap.className = 'sdst-modal';
  wrap.setAttribute('role', 'dialog');
  wrap.setAttribute('aria-modal', 'true');
  wrap.setAttribute('aria-label', existing ? 'Edit saved destination' : 'New saved destination');
  doc.body.appendChild(wrap);

  function fieldHtml(f) {
    var val = values[f.key] == null ? '' : values[f.key];
    if (f.type === 'select') {
      return '<div class="sdst-field"><label for="sdst-' + f.key + '">' + esc(f.label) + '</label>'
        + '<select id="sdst-' + f.key + '" data-key="' + f.key + '">'
        + f.options.map(function (op) { return '<option value="' + esc(op) + '"' + (String(val) === op ? ' selected' : '') + '>' + esc(op) + '</option>'; }).join('')
        + '</select></div>';
    }
    return '<div class="sdst-field"><label for="sdst-' + f.key + '">' + esc(f.label) + '</label>'
      + '<input id="sdst-' + f.key + '" data-key="' + f.key + '" type="text" value="' + esc(val) + '" placeholder="' + esc(f.placeholder || '') + '"'
      + ' autocomplete="off" spellcheck="false"></div>';
  }
  function render() {
    var k = KINDS[kind];
    var kept = existing && hasSecret(existing.id);
    wrap.innerHTML = '<div class="sdst-card">'
      + '<div class="sdst-head">' + (existing ? 'Edit saved destination' : 'New saved destination') + '</div>'
      + '<div class="sdst-body">'
      + '<div class="sdst-err" id="sdst-err" style="display:none"></div>'
      + '<div class="sdst-field"><label for="sdst-kind">Kind</label>'
        + '<select id="sdst-kind"' + ((existing || o.lockKind) ? ' disabled' : '') + '>'
        + KIND_IDS.map(function (id) { return '<option value="' + id + '"' + (id === kind ? ' selected' : '') + '>' + esc(KINDS[id].label) + '</option>'; }).join('')
        + '</select><div class="sdst-hint">' + esc(k.blurb) + '</div></div>'
      + '<div class="sdst-field"><label for="sdst-name">Name</label>'
        + '<input id="sdst-name" data-key="name" type="text" value="' + esc(values.name || '') + '" placeholder="What streams will call it" autocomplete="off"></div>'
      + k.fields.map(fieldHtml).join('')
      + '<div class="sdst-field"><label for="sdst-secret">' + esc(k.secret.label) + '</label>'
        + '<input id="sdst-secret" type="password" value="" placeholder="' + esc(kept ? 'kept — leave empty to keep the stored one' : (k.secret.placeholder || '')) + '" autocomplete="new-password">'
        + '<div class="sdst-hint">Names and endpoints sync between your browsers; the credential stays in this one and is never uploaded. '
        + 'On another browser you will be asked for it again.</div></div>'
      + '</div>'
      + '<div class="sdst-foot"><button type="button" class="sdst-btn" id="sdst-cancel">Cancel</button>'
      + '<button type="button" class="sdst-btn primary" id="sdst-save">' + (existing ? 'Save changes' : 'Save destination') + '</button></div>'
      + '</div>';
    var ks = wrap.querySelector('#sdst-kind');
    if (ks && !ks.disabled) ks.onchange = function () { readForm(); kind = ks.value; render(); };
    wrap.querySelector('#sdst-cancel').onclick = close;
    wrap.querySelector('#sdst-save').onclick = submit;
    var first = wrap.querySelector('#sdst-name');
    if (first) first.focus();
  }
  function readForm() {
    wrap.querySelectorAll('[data-key]').forEach(function (el) { values[el.getAttribute('data-key')] = el.value; });
  }
  function close() { if (wrap.parentNode) wrap.parentNode.removeChild(wrap); }
  function submit() {
    readForm();
    var k = KINDS[kind];
    var secretEl = wrap.querySelector('#sdst-secret');
    var typedSecret = secretEl ? secretEl.value : '';
    var v = Object.assign({}, values);
    v[k.secret.key] = typedSecret;
    var errEl = wrap.querySelector('#sdst-err');
    try {
      var built = build(kind, v, existing);
      var secrets = built.secrets;
      // Editing with the secret left blank keeps what is stored.
      if (!secrets && existing && hasSecret(existing.id)) {
        var m = secretsModule();
        secrets = m.get(existing.id) || null;
      }
      var saved = save(built.entry, secrets);
      close();
      if (typeof o.onSaved === 'function') o.onSaved(saved);
    } catch (e) {
      errEl.textContent = e.message || String(e);
      errEl.style.display = 'block';
    }
  }
  wrap.addEventListener('keydown', function (ev) { if (ev.key === 'Escape') close(); });
  render();
  return { close: close };
}

/* The chip list for Connections: name, kind, endpoint (never a credential),
   with edit / rename / delete. `onchange` is called after any write. */
function renderChips(container, counter, opts) {
  var o = opts || {};
  var doc = root && root.document;
  if (!doc) return;
  var el = typeof container === 'string' ? doc.getElementById(container) : container;
  var ct = typeof counter === 'string' ? doc.getElementById(counter) : counter;
  if (!el) return;
  ensureCss(doc);
  var items = list();
  if (ct) ct.textContent = String(items.length);
  if (!items.length) {
    el.innerHTML = '<span class="sdst-empty">None yet. A stream destination saved here can be picked by name in the Data Stream Designer.</span>';
    return;
  }
  el.innerHTML = items.map(function (e) {
    var ep = endpointOf(e);
    var users = usedBy(e.id).length;
    return '<span class="sdst-chip" data-sdst-id="' + esc(e.id) + '" title="' + esc(kindLabel(e.kind) + (ep ? ' — ' + ep : '')
        + (hasSecret(e.id) ? ' · credential on this browser' : ' · no credential on this browser')
        + (users ? ' · used by ' + users + ' stream' + (users === 1 ? '' : 's') : '')) + '">'
      + '<span class="k">' + esc(kindLabel(e.kind)) + '</span>'
      + '<span class="n">' + esc(e.name) + '</span>'
      + '<button type="button" data-act="edit" title="Edit"><i class="ic ic-edit"></i></button>'
      + '<button type="button" data-act="delete" class="danger" title="Delete"><i class="ic ic-trash"></i></button>'
      + '</span>';
  }).join('');
  el.querySelectorAll('[data-act]').forEach(function (b) {
    b.onclick = function () {
      var id = b.closest('[data-sdst-id]').getAttribute('data-sdst-id');
      var e = byId(id);
      if (!e) return;
      if (b.getAttribute('data-act') === 'edit') {
        openEditor({ entry: e, onSaved: function () { renderChips(el, ct, o); if (o.onchange) o.onchange(); } });
      } else {
        var users = usedBy(id);
        if (users.length) {
          root.alert('"' + e.name + '" is used by ' + users.length + ' stream' + (users.length === 1 ? '' : 's')
            + ' (' + users.map(function (u) { return u.name; }).join(', ') + '). Point them elsewhere first.');
          return;
        }
        if (!root.confirm('Delete "' + e.name + '"?\n\nThe credential stored on this browser for it is removed too.')) return;
        var r = remove(id);
        if (!r.ok) root.alert(r.reason);
        renderChips(el, ct, o);
        if (o.onchange) o.onchange();
      }
    };
  });
}

return {
  SIDE: SIDE, KINDS: KINDS, KIND_IDS: KIND_IDS, SECRET_FIELDS: SECRET_FIELDS,
  isDestination: isDestination, kindLabel: kindLabel, endpointOf: endpointOf, sanitised: sanitised,
  secretFieldOf: secretFieldOf, validate: validate, build: build, prefillFromInline: prefillFromInline,
  list: list, byId: byId, forKind: forKind, hasSecret: hasSecret,
  save: save, rename: rename, remove: remove, usedBy: usedBy,
  openEditor: openEditor, renderChips: renderChips,
};
});
