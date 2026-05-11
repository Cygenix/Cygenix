// cygenix-saved-conn-secrets.js
//
// Local-only secrets store for saved connections.
//
// Why this exists:
//   The main saved-connections blob (`cygenix_saved_connections`) is in
//   the cosmos sync layer's SYNC_KEYS list, which means anything stored
//   under that key gets uploaded to Cosmos DB. Connection strings of the
//   form `mssql://user:password@host:port/db` and Azure Function URLs
//   with `?code=...` therefore end up in cloud storage with secrets
//   intact — not what we want.
//
//   This helper owns the secret half of each saved connection (the
//   password-bearing connection string, or the function key) and stores
//   it under a SEPARATE localStorage key — `cygenix_saved_conn_secrets`
//   — that is deliberately NOT in SYNC_KEYS. Secrets stay on the
//   user's own browser; the synced blob holds names + sanitised
//   metadata only.
//
// Storage shape:
//   localStorage['cygenix_saved_conn_secrets'] = JSON object keyed by
//   the saved-connection's `id` (e.g. 'sconn_1777375369157_08hoy'):
//
//     {
//       "sconn_1777...": { connString: "mssql://user:pwd@host/db" },
//       "sconn_1888...": { fnKey: "abc123..." },
//       ...
//     }
//
//   Direct-mode entries get { connString }. Azure-mode entries get
//   { fnKey } (the fnUrl itself is NOT a secret on its own — it's
//   just an HTTPS endpoint URL — so it stays in the synced blob).
//
// Cross-browser behaviour:
//   On a new browser, the synced blob arrives from Cosmos but the
//   secrets store is empty. The user sees their saved connections by
//   name, but selecting one will leave the connString / fnKey field
//   empty and prompt them to re-enter the secret. That's the trade-off
//   the user accepted: friendly names sync, secrets don't.

(function () {
  'use strict';

  const KEY = 'cygenix_saved_conn_secrets';

  function readAll() {
    try {
      const raw = localStorage.getItem(KEY);
      if (!raw) return {};
      const obj = JSON.parse(raw);
      return (obj && typeof obj === 'object' && !Array.isArray(obj)) ? obj : {};
    } catch { return {}; }
  }

  function writeAll(obj) {
    try {
      localStorage.setItem(KEY, JSON.stringify(obj || {}));
    } catch (e) {
      console.error('[saved-conn-secrets] localStorage write failed:', e);
    }
  }

  // Get the secret bundle for a given saved-connection id.
  // Returns {} (not null) so callers can `const { connString } = get(id)`
  // safely without an existence check.
  function getSecret(id) {
    if (!id) return {};
    const all = readAll();
    return all[id] || {};
  }

  // Replace the secret bundle for a given id. Pass null/undefined to
  // delete. Pass a partial object to merge — but in practice every
  // call from sconnSetAll provides the full bundle for that entry, so
  // we just overwrite.
  function setSecret(id, secrets) {
    if (!id) return;
    const all = readAll();
    if (secrets == null) {
      delete all[id];
    } else {
      all[id] = secrets;
    }
    writeAll(all);
  }

  // Bulk update — used when sconnSetAll rewrites the whole list.
  // Removes any entries whose ids are no longer in `keepIds`, so
  // deleting a saved connection also removes its orphaned secret.
  function pruneTo(keepIds) {
    const keep = new Set(keepIds || []);
    const all = readAll();
    let changed = false;
    for (const id of Object.keys(all)) {
      if (!keep.has(id)) {
        delete all[id];
        changed = true;
      }
    }
    if (changed) writeAll(all);
  }

  // Merge stored secrets back into a sanitised entries array. Mutates
  // the passed array in place — sconnGetAll calls this so that
  // downstream UI code (which expects connString / fnKey to be
  // populated) keeps working without any other changes.
  function rehydrate(entries) {
    if (!Array.isArray(entries)) return entries;
    const all = readAll();
    for (const e of entries) {
      if (!e || !e.id) continue;
      const sec = all[e.id];
      if (!sec) continue;
      if (sec.connString && !e.connString) e.connString = sec.connString;
      if (sec.fnKey      && !e.fnKey)      e.fnKey      = sec.fnKey;
    }
    return entries;
  }

  // Strip secrets out of an entry and return both halves separately.
  // Used by sconnSetAll: the sanitised entry goes to the synced blob,
  // the secret bundle goes to localStorage.
  function split(entry) {
    if (!entry || typeof entry !== 'object') return { sanitised: entry, secrets: null };
    const secrets = {};
    const sanitised = Object.assign({}, entry);
    if ('connString' in sanitised) {
      if (sanitised.connString) secrets.connString = sanitised.connString;
      delete sanitised.connString;
    }
    if ('fnKey' in sanitised) {
      if (sanitised.fnKey) secrets.fnKey = sanitised.fnKey;
      delete sanitised.fnKey;
    }
    return {
      sanitised,
      secrets: Object.keys(secrets).length ? secrets : null,
    };
  }

  // Has-secret check — used by the dashboard UI to show a "secret
  // present" tick or a "needs re-entry on this browser" hint.
  function hasSecret(id) {
    if (!id) return false;
    const s = readAll()[id];
    return !!(s && (s.connString || s.fnKey));
  }

  window.CygenixSavedConnSecrets = {
    get: getSecret,
    set: setSecret,
    pruneTo,
    rehydrate,
    split,
    hasSecret,
  };
})();
