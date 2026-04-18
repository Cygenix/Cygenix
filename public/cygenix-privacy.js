/* cygenix-privacy.js — project privacy & security settings + audit log
 * Consumed by any page that calls Claude (object_mapping, data-cleansing, SQL editor, etc.)
 *
 * Central API exposed on window.CygenixPrivacy:
 *   getSettings()           → current settings object
 *   getMode()               → 'full' | 'schema-only' | 'no-ai'
 *   isAIAllowed(kind)       → bool · kind = 'schema' | 'rowdata' | any string
 *   isColumnExcluded(name)  → bool · tests the exclusion glob list
 *   filterColumns(cols)     → returns a subset with excluded columns removed
 *   redactValue(v)          → obfuscate a value for safe AI display (if redact enabled)
 *   redactRow(row)          → returns a shallow copy with string values redacted
 *   logAIAccess(feature, details) → appends to audit log
 *   getLog()                → returns audit log entries
 *   clearLog()              → wipes audit log
 *   saveSettings(obj)       → persists settings
 *
 * All storage is browser-local (localStorage). No server-side persistence yet.
 */
(function(){
  const SETTINGS_KEY = 'cygenix_privacy_settings';
  const LOG_KEY      = 'cygenix_privacy_log';
  const LOG_MAX      = 500;

  const DEFAULT_SETTINGS = {
    mode: 'full',                         // 'full' | 'schema-only' | 'no-ai'
    redactAIValues: false,                // if true, row values sent to AI are masked
    columnExcludeList: [                  // glob patterns (*-based). Case-insensitive.
      '*password*', '*pwd*', '*secret*', '*ssn*', '*social*security*',
      '*credit*card*', '*cvv*', '*pin*', '*api*key*', '*token*'
    ]
  };

  function getSettings(){
    try {
      const s = JSON.parse(localStorage.getItem(SETTINGS_KEY) || 'null');
      if (!s) return {...DEFAULT_SETTINGS};
      return { ...DEFAULT_SETTINGS, ...s };
    } catch { return {...DEFAULT_SETTINGS}; }
  }
  function saveSettings(obj){
    const merged = { ...getSettings(), ...obj };
    localStorage.setItem(SETTINGS_KEY, JSON.stringify(merged));
    return merged;
  }

  function getMode(){ return getSettings().mode; }

  // kind='rowdata' → only allowed in Full mode.
  // kind='schema'  → allowed in Full + Schema-only, blocked in No AI.
  function isAIAllowed(kind){
    const mode = getMode();
    if (mode === 'no-ai') return false;
    if (mode === 'schema-only' && kind === 'rowdata') return false;
    return true;
  }

  // Glob to regex: '*password*' → /^.*password.*$/i
  function globToRe(pat){
    const esc = String(pat).replace(/[.+?^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*');
    return new RegExp('^' + esc + '$', 'i');
  }

  function isColumnExcluded(name){
    if (!name) return false;
    const list = getSettings().columnExcludeList || [];
    for (const pat of list){
      if (!pat) continue;
      try { if (globToRe(pat).test(name)) return true; } catch {}
    }
    return false;
  }

  // Filter an array of {name,...} or plain string[] — returns the non-excluded subset.
  function filterColumns(cols){
    if (!Array.isArray(cols)) return cols;
    return cols.filter(c => !isColumnExcluded(typeof c === 'string' ? c : c.name));
  }

  // Mask a single value. For redaction in AI payloads. Keep first+last char for short strings,
  // first 2 and last 1 for longer strings. Emails keep domain structure.
  function redactValue(v){
    if (v == null) return v;
    const s = String(v);
    if (!s) return s;
    if (s.length <= 2) return '*'.repeat(s.length);
    // Email
    if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s)){
      const [user, domain] = s.split('@');
      const u = user.length <= 2 ? user[0] + '*' : user[0] + '*'.repeat(user.length - 2) + user[user.length-1];
      const [dn, tld] = domain.split(/\.(.+)/);
      const d = dn.length <= 2 ? dn[0] + '*' : dn[0] + '*'.repeat(dn.length - 2) + dn[dn.length-1];
      return u + '@' + d + '.' + (tld || '');
    }
    // Numeric-only: keep first + last digit
    if (/^\d+$/.test(s)){
      return s.length <= 4 ? s[0] + '*'.repeat(s.length-1) : s[0] + '*'.repeat(s.length-2) + s[s.length-1];
    }
    // General string: first 1-2, last 1
    const prefixLen = s.length >= 6 ? 2 : 1;
    return s.slice(0, prefixLen) + '*'.repeat(Math.max(1, s.length - prefixLen - 1)) + s[s.length-1];
  }

  function redactRow(row){
    if (!row || typeof row !== 'object') return row;
    const out = {};
    for (const k of Object.keys(row)){
      const v = row[k];
      out[k] = (typeof v === 'string' || typeof v === 'number') ? redactValue(v) : v;
    }
    return out;
  }

  function logAIAccess(feature, details){
    try {
      const log = getLog();
      log.push({
        ts: new Date().toISOString(),
        feature: String(feature||'unknown'),
        mode: getMode(),
        details: details || {}
      });
      // Cap to LOG_MAX entries (drop oldest)
      const trimmed = log.slice(-LOG_MAX);
      localStorage.setItem(LOG_KEY, JSON.stringify(trimmed));
    } catch(e){ /* don't let logging failures break AI calls */ }
  }
  function getLog(){
    try { return JSON.parse(localStorage.getItem(LOG_KEY) || '[]'); }
    catch { return []; }
  }
  function clearLog(){ localStorage.removeItem(LOG_KEY); }

  window.CygenixPrivacy = {
    getSettings, saveSettings, getMode,
    isAIAllowed, isColumnExcluded, filterColumns,
    redactValue, redactRow,
    logAIAccess, getLog, clearLog,
    DEFAULT_SETTINGS
  };
})();
