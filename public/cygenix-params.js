// cygenix-params.js
// Shared utility for Cygenix system parameters (@@Token substitution).
// Used by project-builder, object_mapping, SQL editor, etc. to replace
// occurrences of @@ParamName with the parameter's Value before executing.

(function(global){
  const STORAGE_KEY = 'cygenix_sys_params';

  // ── Storage ────────────────────────────────────────────────────────────────
  function getParams(){
    try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]'); }
    catch { return []; }
  }
  function saveParams(list){
    localStorage.setItem(STORAGE_KEY, JSON.stringify(list || []));
  }

  // ── Name → Code derivation ────────────────────────────────────────────────
  // Sanitises a user-typed name into a valid @@Token:
  //   "Start Date"      -> "@@StartDate"
  //   "tenant.id"       -> "@@tenantid"
  //   "Cut-off (2024)"  -> "@@Cutoff2024"
  // Rule: strip anything non-alphanumeric. Keep the user's casing.
  function codeFromName(name){
    const cleaned = String(name||'').replace(/[^A-Za-z0-9]+/g, '');
    return cleaned ? '@@' + cleaned : '';
  }

  // ── Substitution ──────────────────────────────────────────────────────────
  // Replace every @@Token in `text` with the matching parameter's Value.
  // - Case-insensitive matching on the token name
  // - Longest-token-first order so @@StartDate doesn't accidentally match
  //   inside @@StartDateUTC
  // - Unknown tokens are left as-is (makes missing params visible rather
  //   than silently producing bad SQL)
  function substituteParams(text, paramsOverride){
    if (text == null) return text;
    const s = String(text);
    if (!s.includes('@@')) return s;
    const params = paramsOverride || getParams();
    if (!params.length) return s;
    // Sort by code length descending
    const sorted = [...params].sort((a,b) => (b.code||'').length - (a.code||'').length);
    let out = s;
    for (const p of sorted){
      const code = p.code || codeFromName(p.name);
      if (!code) continue;
      // Escape for regex + build case-insensitive pattern.
      // Use a word boundary on the right so @@Foo doesn't match @@FooBar.
      const escaped = code.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const re = new RegExp(escaped + '(?![A-Za-z0-9_])', 'gi');
      out = out.replace(re, p.value != null ? String(p.value) : '');
    }
    return out;
  }

  // ── Deep substitute — recurses into objects/arrays, substitutes strings ────
  // Useful when you want to sub into a whole payload (e.g. a mapping step
  // with embedded SQL and literal values) without hunting every field.
  function substituteParamsDeep(obj, paramsOverride){
    if (obj == null) return obj;
    if (typeof obj === 'string') return substituteParams(obj, paramsOverride);
    if (Array.isArray(obj)) return obj.map(x => substituteParamsDeep(x, paramsOverride));
    if (typeof obj === 'object'){
      const out = {};
      for (const k of Object.keys(obj)) out[k] = substituteParamsDeep(obj[k], paramsOverride);
      return out;
    }
    return obj;
  }

  // ── Scan helper — list every @@Token that appears in `text` ───────────────
  // Useful for "missing parameter" warnings in the UI.
  function findTokens(text){
    if (!text) return [];
    const matches = String(text).match(/@@[A-Za-z0-9_]+/g) || [];
    return [...new Set(matches)];
  }

  // Export
  global.CygenixParams = {
    STORAGE_KEY,
    getParams, saveParams, codeFromName,
    substituteParams, substituteParamsDeep, findTokens
  };
})(typeof window !== 'undefined' ? window : globalThis);
