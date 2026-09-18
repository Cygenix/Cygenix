/* ============================================================================
   cygenix-map-groups.js — named, coloured categories for Object Mapping maps
   ----------------------------------------------------------------------------
   Sep-2026, Phase 1. An analyst building forty maps for one conversion needs
   to see at a glance which of them are master data, which are transactional
   and which are reference — the sort of thing that lives on a whiteboard
   until it lives in the tool. A group is a name and a colour; a map belongs
   to at most one.

   WHAT IS STORED, AND WHERE
   The group LIST is account-wide, under `cygenix_map_groups`, and syncs like
   any other setting. A map stores only `groupId` — never the colour. That is
   the whole reason the list is separate: recolour "Master data" and every map
   in it changes at once, because none of them ever held a colour to go stale.

   A map whose `groupId` names a group that no longer exists displays as
   Ungrouped and is NOT rewritten. Deleting a group is a decision about the
   group, not a licence to touch forty maps; and if the group comes back from
   another device's sync, its maps come back with it.

   THIS FILE IS PURE. No DOM, no network, no direct localStorage: the page
   passes its storage in. That keeps it testable in Node and keeps the one
   place that knows the rules away from the one place that draws them.

   TARGET-AGNOSTIC. The starter groups are the four categories every data
   migration has, whatever system it is migrating to or from. Nothing here
   knows a table, a module or a product.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixMapGroups) root.CygenixMapGroups = api;
})(typeof globalThis !== 'undefined' ? globalThis : (typeof window !== 'undefined' ? window : this), function () {
'use strict';

var MG_KEY = 'cygenix_map_groups';
var MG_VERSION = 1;
var MG_NAME_MAX = 40;

/* ── The palette ──────────────────────────────────────────────────────────
   Ten colours, offered before the custom picker, chosen to stay legible as a
   4px stripe, a 8px dot and a tinted pill on BOTH themes. That rules out the
   two ends: a pastel disappears into the light theme's white, and a near-
   black disappears into the dark theme's. Everything here sits in the middle
   of the range, which is where a colour can do both jobs. */
var PALETTE = [
  { name: 'Pink',   hex: '#E4579A' },
  { name: 'Red',    hex: '#D64545' },
  { name: 'Orange', hex: '#E07B39' },
  { name: 'Amber',  hex: '#D4A017' },
  { name: 'Green',  hex: '#3A9E5F' },
  { name: 'Teal',   hex: '#2A9D9A' },
  { name: 'Blue',   hex: '#3B7DD8' },
  { name: 'Indigo', hex: '#5B5FC7' },
  { name: 'Purple', hex: '#8E5BC7' },
  { name: 'Slate',  hex: '#6B7785' },
];

/* The four an account starts with. Seeded once, on the first read, and
   editable and deletable from that moment on — they are a starting point,
   not a fixed vocabulary. */
var STARTER_GROUPS = [
  { id: 'grp_master', name: 'Master data',   color: '#E4579A', order: 0 },
  { id: 'grp_txn',    name: 'Transactional', color: '#D64545', order: 1 },
  { id: 'grp_ref',    name: 'Reference',     color: '#3B7DD8', order: 2 },
  { id: 'grp_config', name: 'Configuration', color: '#6B7785', order: 3 },
];

// The grey a map with no group wears. A theme variable would be better, but
// this value is handed to inline styles and SVG alike, so it is a colour.
var UNGROUPED_COLOR = '#9AA3AE';
var UNGROUPED_LABEL = 'Ungrouped';

function str(v) { return v == null ? '' : String(v); }
function trim(v) { return str(v).trim(); }
function lower(v) { return trim(v).toLowerCase(); }

function mgId() {
  return 'grp_' + Math.random().toString(36).slice(2, 8) + Date.now().toString(36).slice(-3);
}

function mgNewStore() {
  return { version: MG_VERSION, groups: STARTER_GROUPS.map(function (g) { return Object.assign({}, g); }) };
}

/* ── Colour ───────────────────────────────────────────────────────────────
   Six hex digits, with or without the hash, case-insensitive, normalised to
   an upper-case #RRGGBB. Three-digit shorthand is expanded rather than
   refused: a person typing #f0a means a colour, and turning that into an
   error would be pedantry. Anything else returns null and the caller keeps
   what it had. */
function mgValidateColor(v) {
  var s = trim(v).replace(/^#/, '');
  if (/^[0-9a-f]{3}$/i.test(s)) s = s[0] + s[0] + s[1] + s[1] + s[2] + s[2];
  if (!/^[0-9a-f]{6}$/i.test(s)) return null;
  return '#' + s.toUpperCase();
}

/* A colour as an rgba() string at a given alpha, for the pill tint. Computed
   rather than written twice so a custom colour tints exactly like a palette
   one, and so the tint follows a recolour with no second field to update. */
function mgTint(hex, alpha) {
  var c = mgValidateColor(hex) || UNGROUPED_COLOR;
  var n = parseInt(c.slice(1), 16);
  var a = typeof alpha === 'number' ? alpha : 0.14;
  return 'rgba(' + ((n >> 16) & 255) + ',' + ((n >> 8) & 255) + ',' + (n & 255) + ',' + a + ')';
}

/* ── Names ────────────────────────────────────────────────────────────────
   Required, trimmed, at most forty characters, and unique ignoring case —
   "Master data" and "master data" are the same category however they are
   typed, and two of them on a legend would be a puzzle rather than a list.
   `exceptId` lets a rename keep its own name. */
function mgValidateName(name, groups, exceptId) {
  var n = trim(name);
  if (!n) return { ok: false, reason: 'A group needs a name.' };
  if (n.length > MG_NAME_MAX) {
    return { ok: false, reason: 'A group name can be at most ' + MG_NAME_MAX + ' characters (that one is ' + n.length + ').' };
  }
  var clash = (groups || []).some(function (g) {
    return g && g.id !== exceptId && lower(g.name) === lower(n);
  });
  if (clash) return { ok: false, reason: 'There is already a group called "' + n + '".' };
  return { ok: true, name: n };
}

/* ── The store ────────────────────────────────────────────────────────────
   Anything unreadable, empty or the wrong shape is replaced by the starter
   set. A corrupt blob is not evidence that the user wanted no groups, and
   leaving them with an empty dropdown and no way back would be the worse
   answer. Individual bad rows are dropped; a store that still has groups
   after that is kept as it is. */
function mgNormalise(raw) {
  var doc = raw;
  if (typeof doc === 'string') { try { doc = JSON.parse(doc); } catch (e) { doc = null; } }
  if (!doc || typeof doc !== 'object' || !Array.isArray(doc.groups)) return mgNewStore();

  var seenId = {}, seenName = {};
  var groups = doc.groups.map(function (g, i) {
    if (!g || typeof g !== 'object') return null;
    var id = trim(g.id);
    var name = trim(g.name).slice(0, MG_NAME_MAX);
    var color = mgValidateColor(g.color);
    if (!id || !name || !color) return null;
    if (seenId[id] || seenName[lower(name)]) return null;
    seenId[id] = true; seenName[lower(name)] = true;
    return { id: id, name: name, color: color,
      order: typeof g.order === 'number' && isFinite(g.order) ? g.order : i };
  }).filter(Boolean);

  if (!groups.length) return mgNewStore();
  groups.sort(function (a, b) { return a.order - b.order; });
  groups.forEach(function (g, i) { g.order = i; });
  return { version: MG_VERSION, groups: groups };
}

/* Read and write take the storage in rather than reaching for localStorage,
   so this file has no environment of its own and the tests need no stub
   global. A write that throws — a full quota, a blocked store — is reported
   rather than swallowed: the caller has just been told its change was saved. */
function mgLoad(storage) {
  var raw = null;
  try { raw = storage.getItem(MG_KEY); } catch (e) { raw = null; }
  return mgNormalise(raw);
}
function mgSave(storage, store) {
  var doc = mgNormalise(store);
  try { storage.setItem(MG_KEY, JSON.stringify(doc)); return { ok: true, store: doc }; }
  catch (e) { return { ok: false, store: doc, reason: (e && e.message) || 'The group list could not be saved.' }; }
}

function mgOrdered(store) {
  return ((store && store.groups) || []).slice().sort(function (a, b) { return a.order - b.order; });
}
function mgById(store, id) {
  if (!trim(id)) return null;
  return ((store && store.groups) || []).find(function (g) { return g.id === id; }) || null;
}

/* The group a MAP belongs to, or null. The one place that decides what a
   dangling groupId means, so the card, the legend, the dropdown and the
   filter cannot each decide it differently. */
function mgGroupOf(store, map) {
  return mgById(store, map && map.groupId);
}

/* ── Changes ──────────────────────────────────────────────────────────────
   Each returns { ok, store, group|reason }. A refusal returns the store
   untouched, so a caller that writes whatever comes back cannot half-apply a
   rejected edit. */
function mgAdd(store, input) {
  var s = mgNormalise(store);
  var o = input || {};
  var name = mgValidateName(o.name, s.groups, null);
  if (!name.ok) return { ok: false, store: s, reason: name.reason };
  var color = mgValidateColor(o.color);
  if (!color) return { ok: false, store: s, reason: 'Pick a colour, or enter one as six hex digits.' };
  var group = { id: trim(o.id) || mgId(), name: name.name, color: color, order: s.groups.length };
  if (mgById(s, group.id)) return { ok: false, store: s, reason: 'That group id is already in use.' };
  return { ok: true, store: { version: MG_VERSION, groups: s.groups.concat([group]) }, group: group };
}

function mgUpdate(store, id, patch) {
  var s = mgNormalise(store);
  var cur = mgById(s, id);
  if (!cur) return { ok: false, store: s, reason: 'That group no longer exists.' };
  var p = patch || {};
  var next = Object.assign({}, cur);
  if (p.name !== undefined) {
    var name = mgValidateName(p.name, s.groups, id);
    if (!name.ok) return { ok: false, store: s, reason: name.reason };
    next.name = name.name;
  }
  if (p.color !== undefined) {
    var color = mgValidateColor(p.color);
    if (!color) return { ok: false, store: s, reason: 'That is not a colour — six hex digits, like #3B7DD8.' };
    next.color = color;
  }
  return {
    ok: true, group: next,
    store: { version: MG_VERSION, groups: s.groups.map(function (g) { return g.id === id ? next : g; }) },
  };
}

/* Removing a group does NOT touch the maps in it. They keep a groupId that
   now names nothing, display as Ungrouped by the rule above, and come back
   whole if the group returns — which it can, from another device's sync. */
function mgRemove(store, id) {
  var s = mgNormalise(store);
  if (!mgById(s, id)) return { ok: false, store: s, reason: 'That group no longer exists.' };
  var groups = s.groups.filter(function (g) { return g.id !== id; });
  groups.forEach(function (g, i) { g.order = i; });
  // Every group deleted is still a valid state; mgNormalise would re-seed it,
  // so the empty case is built here rather than passed through it.
  return { ok: true, store: { version: MG_VERSION, groups: groups } };
}

/* How many of these maps are in each group — the legend, and the counts on
   the Browse-all filter chips. Groups nobody uses are left out; a dangling
   groupId counts as ungrouped, exactly as it displays. */
function mgUsage(store, maps) {
  var s = mgNormalise(store);
  var counts = {}, ungrouped = 0;
  (maps || []).forEach(function (m) {
    var g = mgGroupOf(s, m);
    if (g) counts[g.id] = (counts[g.id] || 0) + 1; else ungrouped++;
  });
  return {
    ungrouped: ungrouped,
    used: mgOrdered(s).filter(function (g) { return counts[g.id]; })
      .map(function (g) { return Object.assign({}, g, { count: counts[g.id] }); }),
    counts: counts,
  };
}

return {
  MG_KEY: MG_KEY, MG_VERSION: MG_VERSION, MG_NAME_MAX: MG_NAME_MAX,
  PALETTE: PALETTE, STARTER_GROUPS: STARTER_GROUPS,
  UNGROUPED_COLOR: UNGROUPED_COLOR, UNGROUPED_LABEL: UNGROUPED_LABEL,
  mgId: mgId, mgNewStore: mgNewStore, mgNormalise: mgNormalise,
  mgValidateColor: mgValidateColor, mgValidateName: mgValidateName, mgTint: mgTint,
  mgLoad: mgLoad, mgSave: mgSave,
  mgOrdered: mgOrdered, mgById: mgById, mgGroupOf: mgGroupOf,
  mgAdd: mgAdd, mgUpdate: mgUpdate, mgRemove: mgRemove, mgUsage: mgUsage,
};
});
