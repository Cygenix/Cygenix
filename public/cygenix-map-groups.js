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
// 2 (Sep-2026): the palette was widened from ten colours to twenty, and the
// four starter colours moved apart. Version 1 lists are migrated — carefully:
// see mgMigrate.
var MG_VERSION = 2;
var MG_NAME_MAX = 40;

/* ── The palette ──────────────────────────────────────────
   Twenty colours, offered before the custom picker. The first ten were too
   few and too close together: pink beside red, and a grey for Configuration
   that read as the same dot as Ungrouped. A category colour that has to be
   compared with its neighbour to be identified is not doing the job.

   So these are spread around the wheel at roughly even intervals — about 24°
   between neighbours — rather than picked one at a time. Adjacent entries are
   still the closest pair in the set, but nothing else is, and a group picked
   from opposite ends is unmistakable.

   All of them sit in the middle of the lightness range, which is what lets one
   value work as a 4px stripe, a 9px dot and a tinted pill on BOTH themes: a
   pastel disappears into the light theme's white and a near-black into the
   dark theme's. Brown and Slate at the end are for the categories that
   genuinely are background — and both are darker than the Ungrouped grey, so
   a group is never mistaken for the absence of one. */
var PALETTE = [
  { name: 'Red',     hex: '#D64545' },
  { name: 'Coral',   hex: '#E2673C' },
  { name: 'Orange',  hex: '#E08A2E' },
  { name: 'Amber',   hex: '#D4A017' },
  { name: 'Olive',   hex: '#97A22B' },
  { name: 'Lime',    hex: '#6FA62E' },
  { name: 'Green',   hex: '#3C9E45' },
  { name: 'Emerald', hex: '#219E6C' },
  { name: 'Teal',    hex: '#189B95' },
  { name: 'Cyan',    hex: '#1C93B8' },
  { name: 'Sky',     hex: '#2F86DB' },
  { name: 'Blue',    hex: '#3B62D8' },
  { name: 'Indigo',  hex: '#5B5FC7' },
  { name: 'Violet',  hex: '#8250C9' },
  { name: 'Purple',  hex: '#A548C4' },
  { name: 'Magenta', hex: '#CE4295' },
  { name: 'Pink',    hex: '#E4579A' },
  { name: 'Crimson', hex: '#B32D4E' },
  { name: 'Brown',   hex: '#8A6240' },
  { name: 'Slate',   hex: '#6B7785' },
];

/* The four an account starts with. Seeded once, on the first read, and
   editable and deletable from that moment on — they are a starting point,
   not a fixed vocabulary.

   Their colours are picked to be far apart from EACH OTHER and from the
   Ungrouped grey, not just to look pleasant in a list. The first set was
   pink, red, blue and grey: pink beside red, and a grey that read as the same
   dot as Ungrouped. These four are roughly a quarter-turn apart, and a test
   holds them that way rather than leaving it to whoever edits this next. */
var STARTER_GROUPS = [
  { id: 'grp_master', name: 'Master data',   color: '#E4579A', order: 0 },
  { id: 'grp_txn',    name: 'Transactional', color: '#D4A017', order: 1 },
  { id: 'grp_ref',    name: 'Reference',     color: '#3C9E45', order: 2 },
  { id: 'grp_config', name: 'Configuration', color: '#3B62D8', order: 3 },
];

/* What the four started out as, before the palette was widened. Kept only so
   the migration below can tell an untouched default from a colour somebody
   chose — and for no other purpose, which is why it is not exported. */
var LEGACY_STARTER_COLORS = {
  grp_master: '#E4579A', grp_txn: '#D64545', grp_ref: '#3B7DD8', grp_config: '#6B7785',
};

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
  return mgMigrate({ version: Number(doc.version) || 1, groups: groups });
}

/* ── Migration ────────────────────────────────────────────
   Version 1 seeded four groups whose colours turned out to be too close to
   each other, and one of which — a grey Configuration — read as the same dot
   as Ungrouped. Version 2 moves them apart.

   The migration touches a starter group ONLY while it still has the exact
   colour it was seeded with. That is the test for "nobody has chosen this":
   a colour somebody picked, even if they picked the old default from the
   palette by hand, is a decision and is left alone. Groups the user created
   are never touched, and neither is anything already on version 2.

   A colour is not data the user would miss being corrected, but it is data
   they might have set, so the line is drawn at untouched. */
function mgMigrate(store) {
  if (!store || Number(store.version) >= MG_VERSION) {
    return { version: MG_VERSION, groups: (store && store.groups) || [] };
  }
  var groups = (store.groups || []).map(function (g) {
    var was = LEGACY_STARTER_COLORS[g.id];
    if (!was || g.color !== was) return g;
    var now = STARTER_GROUPS.find(function (d) { return d.id === g.id; });
    return now ? Object.assign({}, g, { color: now.color }) : g;
  });
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
  mgId: mgId, mgNewStore: mgNewStore, mgNormalise: mgNormalise, mgMigrate: mgMigrate,
  mgValidateColor: mgValidateColor, mgValidateName: mgValidateName, mgTint: mgTint,
  mgLoad: mgLoad, mgSave: mgSave,
  mgOrdered: mgOrdered, mgById: mgById, mgGroupOf: mgGroupOf,
  mgAdd: mgAdd, mgUpdate: mgUpdate, mgRemove: mgRemove, mgUsage: mgUsage,
};
});
