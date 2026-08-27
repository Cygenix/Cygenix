/* ============================================================================
   cygenix-page-reader.js — a generic, honest description of the current screen.
   ----------------------------------------------------------------------------
   The assistant's actions are typed and semantic: sql_run, stream_draft,
   app_navigate. That is the right shape for the things it does often, because
   a named action with a schema is auditable and cannot silently change meaning
   when a screen is redesigned.

   It is the wrong shape for the long tail. Cygenix has fifty screens and a few
   thousand controls; only a fraction will ever be worth a typed action. When a
   user asks for something no action covers, the assistant currently has to say
   no — even though the control is right there on the screen it can already see.

   This module is the other half: it reads the page the way a person does.
   It answers ONE question — "what is on this screen, and what can be operated"
   — and it answers it in the vocabulary a person would use (the text on the
   button, the placeholder in the field, the heading above the section) rather
   than in markup.

   WHAT IT IS NOT
   It is not a DOM dump. A page here can carry three thousand nodes; the
   snapshot is capped at MAX_CHARS so a read can never crowd the conversation
   out. It is not an actuator either — nothing here clicks, types or submits.
   Reading is separate from acting on purpose, so the read half can ship,
   be inspected and be trusted before anything is wired to move.

   THE ELEMENT ID
   Every interactive element gets a stable id, `el_` + the first 8 hex digits
   of sha256(accessible name, tag, type, nearest heading). Deliberately absent
   from that hash:

     - The DOM `id` attribute. Most controls here have none, and the ones that
       do are inconsistent about it. An identifier scheme that works for a
       tenth of the page is not a scheme.
     - Screen position. A coordinate changes when the user scrolls, when the
       window is resized, when a row above expands. An id that moves is an id
       that points at the wrong button a second later.

   What is left is what a person uses to find a control again: what it says,
   what kind of thing it is, and what part of the page it sits in. Those
   survive a re-render, a sort, a scroll and a filter. Where they genuinely
   collide — two "Edit" buttons under one heading — the duplicates take a
   _2, _3 suffix in document order, which is stable for the same reason.

   SECRETS DO NOT LEAVE THE SCREEN
   Everything returned here is sent to the model. A password box, a connection
   string field and an API key field are all just <input> to the DOM, so this
   module names them and refuses to read them. See redactValue().

   Node-requirable: the parts that decide things — the hash, the id, the
   redaction, the size cap — are pure and tested without a browser.
   ========================================================================== */
(function (root, factory) {
  var api = factory(root);
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixPageReader) root.CygenixPageReader = api;
})(typeof window !== 'undefined' ? window : this, function (root) {
'use strict';

var MAX_CHARS = 6000;      // the whole snapshot, serialised
var MAX_LABEL = 80;
var MAX_VALUE = 60;
var MAX_LANDMARKS = 25;
var ID_LEN = 8;

/* ── sha256, in the page ────────────────────────────────────────────────────
   crypto.subtle would do this too, but it is async, absent over plain http
   and absent in Node's older shapes — and this has to give the SAME id in the
   browser and in the test suite, or the tests are testing something else.
   Roughly forty lines, no dependency, deterministic everywhere. ─────────── */

var K = [
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
  0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
  0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
  0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
  0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
  0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
  0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
  0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
  0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
];

function rotr(x, n) { return (x >>> n) | (x << (32 - n)); }

function utf8Bytes(str) {
  var out = [], s = String(str);
  for (var i = 0; i < s.length; i++) {
    var c = s.charCodeAt(i);
    if (c < 0x80) out.push(c);
    else if (c < 0x800) out.push(0xc0 | (c >> 6), 0x80 | (c & 63));
    else if (c < 0xd800 || c >= 0xe000) out.push(0xe0 | (c >> 12), 0x80 | ((c >> 6) & 63), 0x80 | (c & 63));
    else {
      var cp = 0x10000 + (((c & 0x3ff) << 10) | (s.charCodeAt(++i) & 0x3ff));
      out.push(0xf0 | (cp >> 18), 0x80 | ((cp >> 12) & 63), 0x80 | ((cp >> 6) & 63), 0x80 | (cp & 63));
    }
  }
  return out;
}

function sha256Hex(str) {
  var H = [0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
           0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19];
  var b = utf8Bytes(str);
  var bits = b.length * 8;
  b = b.slice();
  b.push(0x80);
  while (b.length % 64 !== 56) b.push(0);
  var hi = Math.floor(bits / 4294967296), lo = bits >>> 0;
  b.push((hi >>> 24) & 255, (hi >>> 16) & 255, (hi >>> 8) & 255, hi & 255);
  b.push((lo >>> 24) & 255, (lo >>> 16) & 255, (lo >>> 8) & 255, lo & 255);

  var w = new Array(64), i, t;
  for (i = 0; i < b.length; i += 64) {
    for (t = 0; t < 16; t++) {
      w[t] = (b[i + t * 4] << 24) | (b[i + t * 4 + 1] << 16) | (b[i + t * 4 + 2] << 8) | b[i + t * 4 + 3];
    }
    for (t = 16; t < 64; t++) {
      var s0 = rotr(w[t - 15], 7) ^ rotr(w[t - 15], 18) ^ (w[t - 15] >>> 3);
      var s1 = rotr(w[t - 2], 17) ^ rotr(w[t - 2], 19) ^ (w[t - 2] >>> 10);
      w[t] = (w[t - 16] + s0 + w[t - 7] + s1) | 0;
    }
    var a = H[0], bb = H[1], c = H[2], d = H[3], e = H[4], f = H[5], g = H[6], h = H[7];
    for (t = 0; t < 64; t++) {
      var S1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
      var ch = (e & f) ^ (~e & g);
      var t1 = (h + S1 + ch + K[t] + w[t]) | 0;
      var S0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
      var maj = (a & bb) ^ (a & c) ^ (bb & c);
      var t2 = (S0 + maj) | 0;
      h = g; g = f; f = e; e = (d + t1) | 0; d = c; c = bb; bb = a; a = (t1 + t2) | 0;
    }
    H[0] = (H[0] + a) | 0;  H[1] = (H[1] + bb) | 0; H[2] = (H[2] + c) | 0; H[3] = (H[3] + d) | 0;
    H[4] = (H[4] + e) | 0;  H[5] = (H[5] + f) | 0;  H[6] = (H[6] + g) | 0; H[7] = (H[7] + h) | 0;
  }
  var out = '';
  for (i = 0; i < 8; i++) out += ('0000000' + (H[i] >>> 0).toString(16)).slice(-8);
  return out;
}

/** The id for one element, from the four things that survive a re-render.
 *  Case and spacing are normalised away first: a heading re-cased or
 *  re-indented in a redesign must not renumber every control beneath it. */
function idFor(name, tag, type, section) {
  return 'el_' + sha256Hex([name, tag, type, section].map(function (p) {
    return squash(p).toLowerCase();
  }).join(' ')).slice(0, ID_LEN);
}

/* ── text ──────────────────────────────────────────────────────────────── */

function squash(s) { return String(s == null ? '' : s).replace(/\s+/g, ' ').trim(); }
function clamp(s, n) {
  s = squash(s);
  return s.length > n ? s.slice(0, n - 1) + '…' : s;
}

/* ── redaction ──────────────────────────────────────────────────────────────
   Everything this module returns goes to the model. A password box, a
   connection string and a function key are all <input> as far as the DOM is
   concerned, so the decision has to be made here, on the name and the shape of
   the value, and it has to fail closed. A field whose value is withheld is
   still LISTED — the assistant needs to know the box exists — it just does not
   get to read what is in it. ─────────────────────────────────────────────── */

// Matched against WORDS, not substrings. A field is called fnKey, api_key,
// connString, "Function key" or dbPassword depending on who wrote the screen,
// and a plain substring test either misses the camelCase ones or fires on
// innocent words that happen to contain "key". Splitting the name into words
// first — on camel humps, underscores, hyphens and spaces — catches every
// spelling of the same idea and nothing else.
var SENSITIVE_WORD = /^(pass|passwd|password|passphrase|pwd|secret|secrets|token|tokens|key|keys|apikey|credential|credentials|cred|conn|connstr|connstring|connection|sas|bearer|auth|authorization|signature|sig)$/;
var SENSITIVE_VALUE = /(password|pwd|accountkey|sharedaccesskey|user\s*id)\s*=|^(ey[A-Za-z0-9_-]{20,}|sk-[A-Za-z0-9_-]{16,})/i;

function words(s) {
  return String(s || '')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .split(/[^A-Za-z0-9]+/)
    .filter(Boolean)
    .map(function (w) { return w.toLowerCase(); });
}

function isSensitive(type, name, label) {
  if (String(type).toLowerCase() === 'password') return true;
  return words(String(name || '') + ' ' + String(label || '')).some(function (w) {
    return SENSITIVE_WORD.test(w);
  });
}

/** The value to report for a field, or null when it must not leave the page. */
function redactValue(type, name, label, value) {
  if (value == null || value === '') return null;
  if (isSensitive(type, name, label)) return '(hidden — this field holds a credential)';
  if (SENSITIVE_VALUE.test(String(value))) return '(hidden — this value looks like a credential)';
  return clamp(value, MAX_VALUE);
}

/* ── classification ────────────────────────────────────────────────────── */

/** The five kinds the assistant reasons about. Anything operable is one of them. */
function kindFor(tag, type, role) {
  tag = String(tag || '').toLowerCase();
  role = String(role || '').toLowerCase();
  type = String(type || '').toLowerCase();
  if (role === 'button' || role === 'menuitem' || role === 'tab') return 'button';
  if (role === 'link') return 'link';
  if (tag === 'button') return 'button';
  if (tag === 'a') return 'link';
  if (tag === 'select') return 'select';
  if (tag === 'textarea') return 'textarea';
  if (tag === 'input') return (type === 'button' || type === 'submit' || type === 'reset') ? 'button' : 'input';
  return 'button';
}

/* ── the size cap ───────────────────────────────────────────────────────────
   A snapshot that does not fit is worse than a short one: it displaces the
   conversation it was meant to inform. Elements are dropped from the END, so
   the top of the screen — where the user is looking — always survives, and
   the count that was dropped is reported rather than quietly lost. ─────── */

function withNote(out, dropped) {
  out.truncated = dropped;
  out.note = dropped + ' further element(s) below were not listed. Ask the user to ' +
    'scroll, or narrow what you are looking for.';
  return out;
}

function fit(snapshot, maxChars) {
  var cap = maxChars || MAX_CHARS;
  var out = {
    page_title: snapshot.page_title,
    current_route: snapshot.current_route,
    landmarks: (snapshot.landmarks || []).slice(0, MAX_LANDMARKS),
    interactive_elements: (snapshot.interactive_elements || []).slice()
  };
  // The note that explains the truncation is itself part of the payload, so
  // the fit has to be measured WITH it — otherwise a snapshot trimmed to
  // exactly the cap goes back over it the moment the explanation is added.
  var dropped = 0;
  while (out.interactive_elements.length > 1) {
    if (JSON.stringify(dropped ? withNote(out, dropped) : out).length <= cap) break;
    out.interactive_elements.pop();
    dropped++;
  }
  return dropped ? withNote(out, dropped) : out;
}

/* ══════════════════════════════════════════════════════════════════════════
   The DOM half. Everything above is pure; everything below needs a document.
   ══════════════════════════════════════════════════════════════════════════ */

var SELECTOR = [
  'button', 'a[href]', 'input', 'select', 'textarea',
  '[role="button"]', '[role="link"]', '[role="tab"]', '[role="menuitem"]',
  '[role="checkbox"]', '[role="switch"]',
  '[onclick]', '[contenteditable=""]', '[contenteditable="true"]'
].join(',');

var HEADINGS = 'h1,h2,h3,h4,h5,h6,legend,[role="heading"]';

/* The assistant's own panel is not part of the page it is reading. Listing its
   Send button as something to click is how an agent ends up talking to itself. */
function inOwnPanel(node) {
  return !!(node.closest && node.closest('#cygAssistant, .cyga-launch, .cyga, [data-cyg-chrome]'));
}

function attr(node, name) {
  return (node.getAttribute && node.getAttribute(name)) || '';
}

function visible(node, win) {
  if (!node || node.nodeType !== 1) return false;
  if (node.hasAttribute && node.hasAttribute('hidden')) return false;
  if (attr(node, 'aria-hidden') === 'true') return false;
  if (node.closest && node.closest('[aria-hidden="true"]')) return false;
  if (String(node.type).toLowerCase() === 'hidden') return false;
  var cs = null;
  try { cs = win.getComputedStyle(node); } catch (e) { cs = null; }
  if (cs && (cs.display === 'none' || cs.visibility === 'hidden' || cs.visibility === 'collapse')) return false;
  if (cs && Number(cs.opacity) === 0) return false;
  // offsetParent is null for display:none — and also for position:fixed, which
  // is how every toolbar and modal in this console is anchored. Checking it
  // alone would hide exactly the controls most worth reporting, so a fixed
  // element falls through to the rect test instead.
  if (node.offsetParent === null && (!cs || cs.position !== 'fixed')) return false;
  if (node.getClientRects && node.getClientRects().length === 0) return false;
  return true;
}

/** The name a person would use for this control, in the order they would find it. */
function accessibleName(node, doc) {
  var tag = (node.tagName || '').toLowerCase();

  var aria = squash(attr(node, 'aria-label'));
  if (aria) return aria;

  var by = attr(node, 'aria-labelledby');
  if (by) {
    var parts = by.split(/\s+/).map(function (id) {
      var n = doc.getElementById(id);
      return n ? squash(n.textContent) : '';
    }).filter(Boolean);
    if (parts.length) return parts.join(' ');
  }

  if (tag === 'input' || tag === 'select' || tag === 'textarea') {
    var id = attr(node, 'id');
    if (id) {
      var lab = null;
      try { lab = doc.querySelector('label[for="' + String(id).replace(/"/g, '\\"') + '"]'); }
      catch (e) { lab = null; }
      if (lab) { var t = squash(lab.textContent); if (t) return t; }
    }
    var wrap = node.closest && node.closest('label');
    if (wrap) { var wt = squash(wrap.textContent); if (wt) return wt; }
    var ph = squash(attr(node, 'placeholder'));
    if (ph) return ph;
    var ti = squash(attr(node, 'title'));
    if (ti) return ti;
    var ty = String(node.type || '').toLowerCase();
    if (ty === 'submit' || ty === 'button' || ty === 'reset') {
      var v = squash(node.value);
      if (v) return v;
    }
    var nm = squash(attr(node, 'name'));
    if (nm) return nm;
    return tag === 'select' ? 'dropdown' : (ty || 'text') + ' field';
  }

  var text = squash(node.textContent);
  if (text) return text;

  var title = squash(attr(node, 'title'));
  if (title) return title;

  var img = node.querySelector && node.querySelector('img[alt]');
  if (img) { var alt = squash(attr(img, 'alt')); if (alt) return alt; }

  // An icon-only control with no label at all is a real accessibility problem
  // and worth naming as one rather than papering over.
  return '(unlabelled ' + tag + ')';
}

/** The heading a control sits under — the section a person would name. */
function sectionOf(node) {
  var n = node;
  for (var depth = 0; n && depth < 12; depth++) {
    var sib = n.previousElementSibling;
    for (var back = 0; sib && back < 40; back++) {
      if (sib.matches && sib.matches(HEADINGS)) {
        var t = squash(sib.textContent);
        if (t) return clamp(t, MAX_LABEL);
      }
      // A heading is often the first child of the block above rather than a
      // sibling of the control — a card's <h3> inside the card's header.
      var inner = sib.querySelector && sib.querySelector(HEADINGS);
      if (inner) {
        var it = squash(inner.textContent);
        if (it) return clamp(it, MAX_LABEL);
      }
      sib = sib.previousElementSibling;
    }
    var own = n.getAttribute && (n.getAttribute('aria-label') || '');
    if (own && n.matches && n.matches('section,fieldset,dialog,[role="region"],[role="dialog"]')) {
      return clamp(own, MAX_LABEL);
    }
    n = n.parentElement;
  }
  return '';
}

/** The current address, in the form the rest of the console uses. */
function routeOf(pathname) {
  var p = String(pathname || '/').replace(/\.html$/, '').replace(/\/+$/, '');
  return p || '/';
}

function valueOf(node, tag, label) {
  var type = String(node.type || '').toLowerCase();
  var name = attr(node, 'name') + ' ' + attr(node, 'id');
  if (tag === 'select') {
    var sel = node.options && node.options[node.selectedIndex];
    return sel ? clamp(sel.textContent || sel.value, MAX_VALUE) : null;
  }
  if (type === 'checkbox' || type === 'radio') return node.checked ? 'checked' : 'unchecked';
  if (tag === 'input' || tag === 'textarea') return redactValue(type, name, label, node.value);
  if (attr(node, 'contenteditable')) return redactValue('', name, label, squash(node.textContent));
  return null;
}

/* The result of the most recent read, so a later step can require that an
   element was actually seen before it is operated on. Nothing consults this
   yet — read_page is the only tool that exists — but it is read_page's own
   business to record what it showed, so it does. */
var last = null;
function lastRead() { return last; }

function readPage(opts) {
  opts = opts || {};
  var doc = opts.document || (typeof document !== 'undefined' ? document : null);
  var win = opts.window || root || (typeof window !== 'undefined' ? window : null);
  if (!doc || !win) throw new Error('read_page needs a browser document.');

  // querySelectorAll answers a comma-separated list once per node, in document
  // order, so a control matching two of the selectors appears once.
  var nodes = [];
  try { nodes = Array.prototype.slice.call(doc.querySelectorAll(SELECTOR)); } catch (e) { nodes = []; }

  var landmarks = [], counts = {}, byId = {}, elements = [];

  Array.prototype.slice.call(doc.querySelectorAll(HEADINGS)).forEach(function (h) {
    if (inOwnPanel(h) || !visible(h, win)) return;
    var text = clamp(h.textContent, MAX_LABEL);
    if (!text) return;
    landmarks.push({ heading: text, element_id: idFor(text, (h.tagName || '').toLowerCase(), '', '') });
  });

  nodes.forEach(function (node) {
    if (inOwnPanel(node) || !visible(node, win)) return;

    var tag = (node.tagName || '').toLowerCase();
    var type = String(node.type || attr(node, 'type') || '').toLowerCase();
    var role = attr(node, 'role');
    var label = clamp(accessibleName(node, doc), MAX_LABEL);
    var section = sectionOf(node);

    var base = idFor(label, tag, type, section);
    counts[base] = (counts[base] || 0) + 1;
    var id = counts[base] === 1 ? base : base + '_' + counts[base];

    var entry = { id: id, kind: kindFor(tag, type, role), label: label };
    var value = valueOf(node, tag, label);
    if (value != null) entry.value = value;
    if (section) entry.section = section;
    if (node.disabled) entry.disabled = true;

    elements.push(entry);
    byId[id] = node;
  });

  var snapshot = fit({
    page_title: clamp(doc.title, 120),
    current_route: routeOf(win.location && win.location.pathname),
    landmarks: landmarks,
    interactive_elements: elements
  }, opts.maxChars);

  last = {
    at: Date.now(),
    route: snapshot.current_route,
    ids: snapshot.interactive_elements.map(function (e) { return e.id; }),
    nodes: byId
  };
  return snapshot;
}

return {
  readPage: readPage,
  lastRead: lastRead,
  /* pure core, exported for the tests */
  __core: {
    sha256Hex: sha256Hex,
    idFor: idFor,
    kindFor: kindFor,
    redactValue: redactValue,
    isSensitive: isSensitive,
    routeOf: routeOf,
    clamp: clamp,
    fit: fit,
    SELECTOR: SELECTOR,
    LIMITS: { MAX_CHARS: MAX_CHARS, MAX_LABEL: MAX_LABEL, MAX_VALUE: MAX_VALUE, ID_LEN: ID_LEN }
  }
};
});
