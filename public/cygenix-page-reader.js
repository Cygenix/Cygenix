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
  if (isEditableRegion(node)) return redactValue('', name, label, squash(node.textContent));
  return null;
}

/* The result of the most recent read. click() works only from this: an element
   it can operate is one the assistant has actually looked at, recently, on the
   screen it is still on. */
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

  var entries = {};
  snapshot.interactive_elements.forEach(function (e) { entries[e.id] = e; });
  last = {
    at: Date.now(),
    route: snapshot.current_route,
    ids: snapshot.interactive_elements.map(function (e) { return e.id; }),
    entries: entries,
    nodes: byId
  };
  return snapshot;
}

/* ══════════════════════════════════════════════════════════════════════════
   click — the one thing the assistant may press, and the rules around it.
   ══════════════════════════════════════════════════════════════════════════

   Reading a screen is free. Pressing something on it is not: a button here can
   start a migration, delete a mapping or email a report. So click is fenced on
   four sides, and the fences are independent — a failure of any one of them
   does not open the others.

   1. IT MUST HAVE LOOKED. An id that was not in the most recent read_page, or
      a read older than a minute, or a read taken on a different screen, is
      refused outright with an instruction to read again. The assistant cannot
      press something it has not seen.

   2. THE ELEMENT MUST STILL BE THE SAME ELEMENT. Node identity is not enough:
      a virtualised list recycles its rows, so the node behind "Delete row 1"
      can quietly become "Delete row 500". The live label and section are
      re-derived and compared against what was read, and a mismatch is treated
      as the element being gone.

   3. IRREVERSIBLE THINGS ALWAYS ASK. A label containing delete, remove, drop,
      destroy, confirm, send, submit, publish, pay, purchase or archive
      confirms every time, whatever else is true. The match is a plain
      case-insensitive substring, which over-fires on the odd innocent label
      ("Dropdown") — and over-firing is the correct direction to be wrong in.

   4. EVERYTHING ELSE ASKS TOO, unless it is demonstrably harmless: focusing a
      field, following a link to another screen in this app, or a search or
      filter button — and even then only within thirty seconds of the read it
      came from. Clicking a link is a navigation, which is what app_navigate
      already does with effect `read`, so this is not a hole in the guardrail
      policy: it is the same judgement the catalogue already makes. */

var CLICK_SKIP_MS = 30000;     // a "safe" element may be pressed without asking
var CLICK_MAX_AGE_MS = 60000;  // past this, the read is refused entirely

var IRREVERSIBLE = ['delete', 'remove', 'drop', 'destroy', 'confirm', 'send',
                    'submit', 'publish', 'pay', 'purchase', 'archive'];

/* Deliberately short. Cancel and Close are NOT here: cancelling a half-filled
   form throws the operator's work away, and a control that only might be
   harmless belongs on the asking side of the line. */
var SAFE_BUTTON = /^(search|find|filter|refresh|reload|sort)\b|^(apply|clear|reset)\s+(filter|filters|search)\b/i;

/** The word that puts a label on the irreversible list, or null. */
function irreversible(label) {
  var l = String(label || '').toLowerCase();
  for (var i = 0; i < IRREVERSIBLE.length; i++) {
    if (l.indexOf(IRREVERSIBLE[i]) !== -1) return IRREVERSIBLE[i];
  }
  return null;
}

/** Whether this element is one of the few that may be pressed without asking. */
function safeToClick(entry, inApp) {
  if (!entry) return false;
  if (irreversible(entry.label)) return false;
  if (entry.kind === 'input' || entry.kind === 'textarea' || entry.kind === 'select') return true;
  if (entry.kind === 'link') return !!inApp;
  if (entry.kind === 'button') return SAFE_BUTTON.test(String(entry.label || ''));
  return false;
}

/** Does pressing this need the user's word first? Pure, so it is testable. */
function decideConfirm(entry, ageMs, inApp) {
  var word = irreversible(entry && entry.label);
  if (word) {
    return { confirm: true, reason: 'The label contains "' + word + '", which is on the ' +
      'irreversible-action list — this always asks, whatever else is true.' };
  }
  if (!safeToClick(entry, inApp)) {
    return { confirm: true, reason: 'This is not one of the controls that may be pressed ' +
      'without asking (a field, a link within the app, or a search or filter button).' };
  }
  if (ageMs > CLICK_SKIP_MS) {
    return { confirm: true, reason: 'The screen was last read ' + Math.round(ageMs / 1000) +
      ' seconds ago, so what is under this control may have moved.' };
  }
  return { confirm: false, reason: '' };
}

/* ── looking the element back up ───────────────────────────────────────── */

function hrefInfo(node, win) {
  var raw = attr(node, 'href');
  if (!raw || /^(javascript:|#|mailto:|tel:)/i.test(raw)) return { inApp: false, url: null };
  if (attr(node, 'target') && attr(node, 'target') !== '_self') return { inApp: false, url: null };
  var abs = node.href || raw;
  try {
    var u = new win.URL(abs, win.location.href);
    if (u.origin !== win.location.origin) return { inApp: false, url: null };
    return { inApp: true, url: u.pathname + u.search + u.hash };
  } catch (e) { return { inApp: false, url: null }; }
}

function Refusal(message) { var e = new Error(message); e.refusal = true; return e; }

/**
 * Resolve an element id against the last read, or explain why it cannot be.
 * Returns { entry, node, inApp, url, ageMs } or throws a refusal whose message
 * is written for the model — it always says what to do next.
 */
function resolve(elementId, opts) {
  opts = opts || {};
  var doc = opts.document || (typeof document !== 'undefined' ? document : null);
  var win = opts.window || root || (typeof window !== 'undefined' ? window : null);
  var id = String(elementId || '');

  if (!last) throw Refusal('There is no read_page result to work from. Call read_page first.');
  var ageMs = Date.now() - last.at;
  if (ageMs > CLICK_MAX_AGE_MS) {
    throw Refusal('The last read_page is ' + Math.round(ageMs / 1000) + ' seconds old, which is ' +
      'over the ' + (CLICK_MAX_AGE_MS / 1000) + '-second limit. Call read_page again.');
  }
  if (!Object.prototype.hasOwnProperty.call(last.entries, id)) {
    throw Refusal('"' + id + '" was not in the last read_page result. Call read_page again ' +
      'and use an id from it.');
  }
  if (doc && win && routeOf(win.location && win.location.pathname) !== last.route) {
    throw Refusal('The screen has changed since that read_page (it was taken on ' +
      last.route + '). Call read_page again.');
  }

  var entry = last.entries[id];
  var node = last.nodes[id];
  var gone = 'Element ' + id + ' no longer exists; call read_page again';

  if (!node || !doc || !doc.contains(node) || !visible(node, win)) throw Refusal(gone);

  // Node identity is not enough. A virtualised list recycles its rows, so the
  // node that was "Delete row 1" can now be "Delete row 500" — same object,
  // different meaning. If what it says has changed, it is not the element the
  // assistant looked at.
  var liveLabel = clamp(accessibleName(node, doc), MAX_LABEL);
  if (liveLabel !== entry.label || sectionOf(node) !== (entry.section || '')) throw Refusal(gone);

  var href = hrefInfo(node, win);
  return { entry: entry, node: node, ageMs: ageMs, inApp: href.inApp, url: href.url };
}

/**
 * The confirmation question, answered without touching anything. The runtime
 * asks this BEFORE it runs the handler, so it has to be synchronous and it has
 * to be safe to call when the id is stale — in which case it declines to
 * confirm and lets click() throw the refusal that explains why.
 */
function clickConfirmation(elementId) {
  var r;
  try { r = resolve(elementId); }
  catch (e) { return { confirm: false, reason: e.message, label: String(elementId), error: true }; }
  var d = decideConfirm(r.entry, r.ageMs, r.inApp);
  return { confirm: d.confirm, reason: d.reason, label: r.entry.label, error: false };
}

/** Press it. Refuses, loudly and with instructions, rather than guessing. */
function click(elementId, opts) {
  var r = resolve(elementId, opts);
  var entry = r.entry, node = r.node;

  if (node.disabled || node.getAttribute('aria-disabled') === 'true') {
    throw Refusal('Element ' + elementId + ' ("' + entry.label + '") is disabled and cannot be ' +
      'clicked. Tell the user what it is waiting for rather than trying again.');
  }

  // A link is a navigation, and this console persists a run across navigations
  // rather than letting the browser drop it. Hand it back to the runtime the
  // same way app_navigate does instead of following it here.
  if (entry.kind === 'link' && r.inApp && r.url) {
    return { __navigate: r.url,
             __result: 'Clicked "' + entry.label + '" and opened ' + r.url + '.' };
  }

  try { if (node.focus) node.focus(); } catch (e) { /* not focusable; click anyway */ }
  node.click();

  return {
    ok: true,
    clicked: entry.label,
    kind: entry.kind,
    note: 'The screen may have changed. Call read_page again to see what it looks like now ' +
      'before deciding anything else.'
  };
}

/* ══════════════════════════════════════════════════════════════════════════
   type — putting text into a field.
   ══════════════════════════════════════════════════════════════════════════

   Typing is the quietest of the three and, on this console, the one with the
   sharpest edge. Filling in a table name is nothing. Filling in a password box
   is the assistant writing a credential into the operator's browser — and this
   module already refuses to READ those fields, so writing to one without
   asking would be an odd place to stop being careful.

   So type inherits every fence click has (it must have looked, and the field
   must still be the field it looked at), narrows the target to the three
   things that hold text, and asks first when the field is a credential field.
   Everything else goes straight in, which is what makes it useful.

   What it never does is repeat the text back. The model supplied it, so the
   conversation already has it; there is nothing to gain by putting it in the
   result as well, and if it is a credential there is something to lose. */

var MAX_TYPE_CHARS = 10000;

/* Types that live on an <input> but hold no text. A file picker is the one
   that matters: its value cannot be set from script by design, and a browser
   that let it be would be a browser with a hole in it. */
var NON_TEXT_INPUT = /^(checkbox|radio|file|button|submit|reset|image|range|color)$/;

/* attr() cannot answer this: it returns '' both for contenteditable="" — which
   means editable — and for an element with no such attribute at all. Reading
   the attribute directly is the only way to tell those apart, and getting it
   wrong the generous way would make every <div> on the page a text field. */
function isEditableRegion(node) {
  if (!node || !node.getAttribute) return false;
  if (node.isContentEditable === true) return true;
  var ce = node.getAttribute('contenteditable');
  return ce != null && String(ce).toLowerCase() !== 'false';
}

function isTextField(node) {
  var tag = (node.tagName || '').toLowerCase();
  if (tag === 'textarea') return true;
  if (isEditableRegion(node)) return true;
  if (tag !== 'input') return false;
  return !NON_TEXT_INPUT.test(String(node.type || 'text').toLowerCase());
}

/** Is this field one whose contents are a secret? Same rule the reader uses. */
function isCredentialField(node, entry) {
  return isSensitive(
    String(node.type || ''),
    attr(node, 'name') + ' ' + attr(node, 'id'),
    (entry && entry.label) || ''
  );
}

/**
 * Does putting text in this field need the user's word first? Two cases, and
 * both of them are about the operator seeing what is happening to their own
 * browser rather than about the text being a change to any system — nothing
 * typed here reaches a database until something is pressed, and that press
 * has its own confirmation.
 */
function decideTypeConfirm(entry, isCredential) {
  if (isCredential) {
    return { confirm: true, reason: 'This field holds a credential. Nothing is read back out ' +
      'of it, and it is worth checking that what is about to go in is what you expect — ' +
      'passwords and keys are usually better typed by you.' };
  }
  var word = irreversible(entry && entry.label);
  if (word) {
    return { confirm: true, reason: 'The field\'s label contains "' + word + '", which is on ' +
      'the irreversible-action list.' };
  }
  return { confirm: false, reason: '' };
}

/** The confirmation question for type, answered without touching anything. */
function typeConfirmation(elementId) {
  var r;
  try { r = resolve(elementId); }
  catch (e) { return { confirm: false, reason: e.message, label: String(elementId), error: true }; }
  var d = decideTypeConfirm(r.entry, isCredentialField(r.node, r.entry));
  return { confirm: d.confirm, reason: d.reason, label: r.entry.label, error: false };
}

/* Frameworks that track an input's value do it by patching the property on the
   instance, so a plain assignment can be swallowed. Going through the
   prototype's own setter is what React's own test utilities do, and it is
   harmless everywhere else. */
function setValue(node, text, win) {
  var proto = null;
  try {
    var tag = (node.tagName || '').toLowerCase();
    if (tag === 'textarea' && win.HTMLTextAreaElement) proto = win.HTMLTextAreaElement.prototype;
    else if (tag === 'input' && win.HTMLInputElement) proto = win.HTMLInputElement.prototype;
    var d = proto && Object.getOwnPropertyDescriptor(proto, 'value');
    if (d && d.set) { d.set.call(node, text); return; }
  } catch (e) { /* fall through */ }
  node.value = text;
}

function fire(node, win, name) {
  try {
    node.dispatchEvent(new win.Event(name, { bubbles: true }));
  } catch (e) {
    try {
      var ev = (win.document || document).createEvent('Event');
      ev.initEvent(name, true, false);
      node.dispatchEvent(ev);
    } catch (e2) { /* nothing more to try */ }
  }
}

/** Replace a field's contents. Refuses anything that is not a text field. */
function type(elementId, text, opts) {
  opts = opts || {};
  var win = opts.window || root || (typeof window !== 'undefined' ? window : null);
  var value = text == null ? '' : String(text);
  if (value.length > MAX_TYPE_CHARS) {
    throw Refusal('That is ' + value.length + ' characters, over the ' + MAX_TYPE_CHARS +
      '-character limit for one field. Shorten it, or ask the user to paste it themselves.');
  }

  var r = resolve(elementId, opts);
  var entry = r.entry, node = r.node;

  if (!isTextField(node)) {
    throw Refusal('Element ' + elementId + ' ("' + entry.label + '") is a ' + entry.kind +
      ' and holds no text. type works on a text field, a text area or an editable region ' +
      'only. To operate this one, use click.');
  }
  if (node.disabled || node.readOnly || node.getAttribute('aria-disabled') === 'true') {
    throw Refusal('Element ' + elementId + ' ("' + entry.label + '") is read-only or disabled. ' +
      'Tell the user what it is waiting for rather than trying again.');
  }

  try { if (node.focus) node.focus(); } catch (e) { /* carry on */ }

  if (isEditableRegion(node)) {
    node.textContent = value;                 // clears and replaces in one go
  } else {
    setValue(node, '', win);                  // clear first, as a person would
    setValue(node, value, win);
  }
  fire(node, win, 'input');
  fire(node, win, 'change');

  // Deliberately no echo of the text. The model supplied it, so the
  // conversation has it already; repeating it back gains nothing and, in a
  // credential field, loses something.
  return {
    ok: true,
    typed_into: entry.label,
    characters: value.length,
    note: 'The field now holds what you sent. Nothing has been saved or submitted — ' +
      'that still needs a click. Call read_page again if you need to see the result.'
  };
}

/* ══════════════════════════════════════════════════════════════════════════
   wait_for_change — pausing until the page reacts.
   ══════════════════════════════════════════════════════════════════════════

   The other three tools act on a screen that is standing still. This one
   exists because it usually is not: a click starts a query, a save posts to a
   function, a filter re-renders a grid, and the useful thing to do next is
   nothing at all for a moment.

   Without it the assistant has two bad options — read again immediately and
   describe the old screen as though it were the new one, or spin in a
   read-read-read loop until the brakes stop it. Both end with the user being
   told something untrue.

   FOUR SIGNALS, IN ORDER OF HOW MUCH THEY MEAN
     1. The address changed.               Unambiguous: this is a new screen.
     2. A control that was there has gone. A row deleted, a dialog closed, a
                                           button replaced by a spinner.
     3. A heading that was not there is.   A new section, a new panel, a
                                           results pane that has arrived.
     4. Text matching the description.     The loosest, and last for that
                                           reason.

   A TIMEOUT IS NOT AN ERROR. If the page did not change, that is a fact, and
   reporting it as a failed tool call would push the model towards retrying
   when what it should do is tell the user nothing happened. It returns
   {changed: false} calmly and says how long it waited.

   Nothing here touches the page. The observer is disconnected on every exit,
   including the timeout, so a wait cannot outlive the turn that started it. */

var WAIT_DEFAULT_MS = 5000;
var WAIT_MIN_MS = 250;
var WAIT_MAX_MS = 30000;   // the panel is blocked while this runs
var WAIT_TICK_MS = 120;

/* Words that describe the EVENT rather than the thing that appears. "a success
   message appears" is looking for "success message", not for "appears". */
var WAIT_STOPWORDS = {
  the: 1, a: 1, an: 1, and: 1, or: 1, of: 1, to: 1, in: 1, on: 1, for: 1, with: 1,
  is: 1, are: 1, was: 1, be: 1, it: 1, its: 1, that: 1, this: 1, then: 1, when: 1,
  appear: 1, appears: 1, appeared: 1, appearing: 1, show: 1, shows: 1, shown: 1,
  showing: 1, display: 1, displays: 1, displayed: 1, page: 1, screen: 1, view: 1,
  change: 1, changes: 1, changed: 1, update: 1, updates: 1, updated: 1, new: 1,
  after: 1, before: 1, until: 1, some: 1, any: 1, from: 1, into: 1, has: 1, have: 1
};

/** The words in a description worth looking for. Pure, and capped. */
function significantWords(description) {
  var seen = {}, out = [];
  words(description).forEach(function (w) {
    if (w.length < 4 || WAIT_STOPWORDS[w] || seen[w]) return;
    seen[w] = 1;
    if (out.length < 8) out.push(w);
  });
  return out;
}

/**
 * Does this newly-appeared text look like what the model was waiting for?
 *
 * Deliberately generous: ANY significant word counts. A description is a
 * sentence a model wrote, not a selector, and the two ways to be wrong are not
 * equal. A false match returns early saying exactly which word it saw, and the
 * model reads the page and finds out; a false timeout tells it nothing
 * happened when something did, which is the answer it cannot recover from.
 */
function looselyMatches(text, description) {
  var hay = squash(text).toLowerCase();
  if (!hay) return null;
  var whole = squash(description).toLowerCase();
  if (whole && hay.indexOf(whole) !== -1) return whole;
  var wanted = significantWords(description);
  for (var i = 0; i < wanted.length; i++) {
    if (hay.indexOf(wanted[i]) !== -1) return wanted[i];
  }
  return null;
}

function waitForChange(description, timeoutMs, opts) {
  opts = opts || {};
  var doc = opts.document || (typeof document !== 'undefined' ? document : null);
  var win = opts.window || root || (typeof window !== 'undefined' ? window : null);
  if (!doc || !win || !doc.body) throw new Error('wait_for_change needs a browser document.');

  var desc = squash(description);
  var ms = Number(timeoutMs);
  ms = Math.max(WAIT_MIN_MS, Math.min(WAIT_MAX_MS, isFinite(ms) && ms > 0 ? ms : WAIT_DEFAULT_MS));

  // The baseline. Nodes are held directly rather than by id: this only has to
  // answer "is what was here still here", which needs no hashing.
  var startedAt = Date.now();
  var url0 = String(win.location && win.location.href || '');
  var headings0 = {};
  var watched = [];
  try {
    Array.prototype.slice.call(doc.querySelectorAll(HEADINGS)).forEach(function (h) {
      if (inOwnPanel(h) || !visible(h, win)) return;
      var t = clamp(h.textContent, MAX_LABEL);
      if (t) headings0[t.toLowerCase()] = t;
    });
    Array.prototype.slice.call(doc.querySelectorAll(SELECTOR)).forEach(function (n) {
      if (inOwnPanel(n) || !visible(n, win)) return;
      if (watched.length < 400) watched.push({ node: n, label: clamp(accessibleName(n, doc), MAX_LABEL) });
    });
  } catch (e) { /* an unreadable page still gets a timeout rather than a crash */ }

  return new Promise(function (resolve) {
    var done = false, obs = null, timer = null, ticker = null, added = '';

    function finish(what) {
      if (done) return;
      done = true;
      if (obs) { try { obs.disconnect(); } catch (e) {} }
      if (timer) clearTimeout(timer);
      if (ticker) clearInterval(ticker);
      resolve({
        changed: !!what,
        what: what || null,
        waited_ms: Date.now() - startedAt,
        description: desc,
        note: what
          ? 'Something happened, but this only saw that much. Call read_page to find out ' +
            'what the screen says now.'
          : 'Nothing on the page changed in ' + Math.round((Date.now() - startedAt) / 1000) +
            ' seconds. That is a result, not a failure: say so rather than waiting again. ' +
            'If you expected a change, check whether the thing you pressed actually did ' +
            'anything before pressing it a second time.'
      });
    }

    function evaluate() {
      if (done) return;
      var text = added; added = '';

      var url = String(win.location && win.location.href || '');
      if (url !== url0) return finish('the address changed to ' + routeOf(win.location.pathname));

      for (var i = 0; i < watched.length; i++) {
        var w = watched[i];
        if (!doc.contains(w.node) || !visible(w.node, win)) {
          return finish('"' + w.label + '" is no longer on the screen');
        }
      }

      var fresh = null;
      try {
        var hs = Array.prototype.slice.call(doc.querySelectorAll(HEADINGS));
        for (var j = 0; j < hs.length && !fresh; j++) {
          if (inOwnPanel(hs[j]) || !visible(hs[j], win)) continue;
          var t = clamp(hs[j].textContent, MAX_LABEL);
          if (t && !headings0[t.toLowerCase()]) fresh = t;
        }
      } catch (e) { /* ignore */ }
      if (fresh) return finish('a new section appeared: "' + fresh + '"');

      if (desc && text) {
        var hit = looselyMatches(text, desc);
        if (hit) return finish('text mentioning "' + hit + '" appeared');
      }
    }

    try {
      obs = new win.MutationObserver(function (records) {
        for (var i = 0; i < records.length && added.length < 20000; i++) {
          var r = records[i];
          if (r.type === 'characterData') { added += ' ' + (r.target && r.target.data || ''); continue; }
          for (var k = 0; k < (r.addedNodes ? r.addedNodes.length : 0); k++) {
            var n = r.addedNodes[k];
            added += ' ' + (n.textContent || n.nodeValue || '');
          }
        }
      });
      obs.observe(doc.body, { childList: true, subtree: true, characterData: true });
    } catch (e) { obs = null; }

    // The observer catches text arriving. It does NOT catch a pushState, or a
    // control hidden by a stylesheet class rather than a node being removed —
    // so the poll is what actually decides, and the observer only feeds it.
    ticker = setInterval(evaluate, WAIT_TICK_MS);
    timer = setTimeout(function () { finish(null); }, ms);
  });
}

return {
  readPage: readPage,
  lastRead: lastRead,
  waitForChange: waitForChange,
  click: click,
  clickConfirmation: clickConfirmation,
  type: type,
  typeConfirmation: typeConfirmation,
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
    irreversible: irreversible,
    safeToClick: safeToClick,
    decideConfirm: decideConfirm,
    decideTypeConfirm: decideTypeConfirm,
    isTextField: isTextField,
    isCredentialField: isCredentialField,
    significantWords: significantWords,
    looselyMatches: looselyMatches,
    words: words,
    SELECTOR: SELECTOR,
    IRREVERSIBLE: IRREVERSIBLE,
    NON_TEXT_INPUT: NON_TEXT_INPUT,
    LIMITS: { MAX_CHARS: MAX_CHARS, MAX_LABEL: MAX_LABEL, MAX_VALUE: MAX_VALUE, ID_LEN: ID_LEN,
              CLICK_SKIP_MS: CLICK_SKIP_MS, CLICK_MAX_AGE_MS: CLICK_MAX_AGE_MS,
              MAX_TYPE_CHARS: MAX_TYPE_CHARS, WAIT_DEFAULT_MS: WAIT_DEFAULT_MS,
              WAIT_MIN_MS: WAIT_MIN_MS, WAIT_MAX_MS: WAIT_MAX_MS }
  }
};
});
