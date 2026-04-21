// cygenix-params.js
// Shared utility for Cygenix system parameters (@@Token substitution).
// Used by project-builder, object_mapping, SQL editor, etc. to replace
// occurrences of @@ParamName with the parameter's Value before executing.

(function(global){
  const STORAGE_KEY = 'cygenix_sys_params';

  // Supported parameter types. Any value outside this set is treated as
  // the default ('text') to keep old unmigrated params working.
  const PARAM_TYPES = ['number', 'text', 'date', 'raw'];
  const DEFAULT_TYPE = 'text';

  // ── Storage ────────────────────────────────────────────────────────────────
  function getParams(){
    try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]'); }
    catch { return []; }
  }
  function saveParams(list){
    localStorage.setItem(STORAGE_KEY, JSON.stringify(list || []));
  }

  // ── Type handling ─────────────────────────────────────────────────────────
  // Returns a concrete type for a param, defaulting sensibly when missing.
  // A value that parses as a number with no stored type is treated as 'number'
  // so existing untyped rows that happen to hold numbers keep emitting
  // unquoted values. Everything else defaults to 'text'.
  function typeOf(p){
    const t = p && p.type ? String(p.type).toLowerCase() : '';
    if (PARAM_TYPES.includes(t)) return t;
    const v = p && p.value != null ? String(p.value).trim() : '';
    if (v !== '' && /^-?\d+(\.\d+)?$/.test(v)) return 'number';
    return DEFAULT_TYPE;
  }

  // Format a parameter's value for substitution into SQL based on its type.
  //   number → as-is, unquoted (trimmed). Non-numeric values fall through
  //            to text formatting so a mistyped 'abc' doesn't blow up silently.
  //   text   → 'value', with embedded single quotes doubled ('O''Brien')
  //   date   → same quoting as text. Users store dates as '20260101',
  //            '2026-01-01', '31/12/2019' etc — SQL Server converts at runtime.
  //   raw    → passed through verbatim. Escape hatch for NULL, GETDATE(),
  //            SUSER_SNAME(), subqueries, or any expression.
  function formatValue(p){
    const raw = p && p.value != null ? String(p.value) : '';
    switch (typeOf(p)){
      case 'number': {
        const trimmed = raw.trim();
        if (/^-?\d+(\.\d+)?$/.test(trimmed)) return trimmed;
        // Value doesn't look numeric — fall through to quoted text so we
        // don't emit invalid SQL. The dashboard UI flags this as a warning.
        return "'" + raw.replace(/'/g, "''") + "'";
      }
      case 'raw':
        return raw;
      case 'date':
      case 'text':
      default:
        return "'" + raw.replace(/'/g, "''") + "'";
    }
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
  // Replace every @@Token in `text` with the matching parameter's formatted
  // value. Formatting is driven by each param's `type` field:
  //   number → unquoted; text/date → quoted with embedded ' escaped; raw →
  //   passed through verbatim for things like GETDATE() / NULL.
  //
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
      out = out.replace(re, formatValue(p));
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
    PARAM_TYPES, DEFAULT_TYPE,
    getParams, saveParams, codeFromName,
    typeOf, formatValue,
    substituteParams, substituteParamsDeep, findTokens
  };
})(typeof window !== 'undefined' ? window : globalThis);
