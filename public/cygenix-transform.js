/* ============================================================================
   cygenix-transform.js — what a column transform actually does to a value.
   ----------------------------------------------------------------------------
   THE FAILURE THIS FIXES

   A transform was defined in two places that disagreed.

   The Object Mapping editor GENERATES SQL: CAST becomes
   `CAST(src AS NVARCHAR(8))`, SAFE_TRUNC becomes `LEFT(src, 8)`, SAFE_NUMERIC
   becomes `ISNULL(TRY_CAST(… AS INT), 0)`. Those expressions do the work
   inside SQL Server.

   The Batches runner does NOT execute that SQL. It reads source rows into the
   browser, applies the transform in JavaScript, and emits literals. And in
   JavaScript, CAST was a deliberate no-op — the reasoning being that
   SQL Server would cast the literal on insert. That holds for widening
   conversions (N'123' into an INT column) and fails for the narrowing case
   that matters: a literal longer than an NVARCHAR(8) column does not
   truncate, it raises

       String or binary data would be truncated in table '…', column 'clcountry'

   So a mapping that said "CAST to NVARCHAR(8)" produced SQL that worked and a
   run that failed. There was a length safety net further down, but it was
   gated on m.srcType / m.tgtType, which the editor never writes onto a
   mapping — so it never fired.

   One definition, used by both the forecast and the run, is the only way
   those two stay in step. Preflight applies these same rules before it judges
   a value, so it stops forecasting rejections the mapping already handles.

   WHAT IS DELIBERATELY NOT HERE

   Was/Is substitution and @@parameter expansion need page context (the rule
   set, the parameter store), so they stay with their callers and run either
   side of this. This module is the pure value → value part.

   Node-requirable, because a rule that governs both the forecast and the load
   has to be tested on its own.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && !root.CygenixTransform) root.CygenixTransform = api;
})(typeof window !== 'undefined' ? window : this, function () {
'use strict';

/* Type predicates — same expressions the editor's generator uses, so the two
   agree on what "a character type" is. */
function isCharType(t) { return /^(N?VARCHAR|N?CHAR|TEXT|NTEXT)/i.test(String(t || '').trim()); }
function isNumericType(t) {
  return /^(TINYINT|SMALLINT|INT|BIGINT|BIT|DECIMAL|NUMERIC|MONEY|SMALLMONEY|FLOAT|REAL)\b/i
    .test(String(t || '').trim());
}
function isGuidType(t) { return /^UNIQUEIDENTIFIER\b/i.test(String(t || '').trim()); }
function isDateType(t) {
  return /^(DATE|TIME|DATETIME(2)?|SMALLDATETIME|DATETIMEOFFSET)\b/i.test(String(t || '').trim());
}

/* NVARCHAR(8) → 8. NVARCHAR(MAX) and an unsized type → null, meaning
   "no length limit to enforce". */
function typeLength(t) {
  var m = String(t || '').match(/\(\s*(MAX|\d+)\s*\)/i);
  if (!m) return null;
  if (m[1].toUpperCase() === 'MAX') return null;
  var n = parseInt(m[1], 10);
  return isFinite(n) && n > 0 ? n : null;
}

var UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function isBlank(v) {
  return v === null || v === undefined || String(v).trim() === '';
}
function numericOf(v) {
  if (typeof v === 'number') return isFinite(v) ? v : null;
  if (typeof v === 'boolean') return v ? 1 : 0;
  var s = String(v).trim();
  if (!s || !/^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/.test(s)) return null;
  var n = Number(s);
  return isFinite(n) ? n : null;
}

/**
 * Apply one mapping's transform to one value.
 *
 * @param m       the mapping row: { transform, srcType, tgtType }
 * @param v       the source value, AFTER Was/Is and literal handling
 * @param opts    { tgtType } — the live target column type, which beats
 *                m.tgtType. The editor does not save types onto a mapping, so
 *                the runner passes what it introspected from the target and
 *                preflight passes what it already read.
 * @returns { value, truncated, coerced, note }
 *          truncated — a character value was shortened to fit
 *          coerced   — an unusable value became the type's fallback
 */
function txApply(m, v, opts) {
  var o = opts || {};
  var t = String((m && m.transform) || 'NONE').toUpperCase();
  var tgtType = o.tgtType || (m && m.tgtType) || '';
  var srcType = (m && m.srcType) || '';
  var out = v;
  var truncated = false, coerced = false, note = null;

  // Plain string transforms, unchanged.
  if (out != null && (t === 'TRIM' || t === 'UPPER' || t === 'LOWER')) {
    var s0 = String(out);
    if (t === 'TRIM')  out = s0.trim();
    if (t === 'UPPER') out = s0.toUpperCase();
    if (t === 'LOWER') out = s0.toLowerCase();
  }

  // SAFE_GUID — ISNULL(TRY_CAST(NULLIF(TRIM(src),'') AS UNIQUEIDENTIFIER), NEWID())
  // Empties and unparseable values both become a fresh GUID, matching the
  // generator. A GUID minted here is minted once per value, which is what the
  // SQL does per row.
  if (t === 'SAFE_GUID') {
    var g = isBlank(out) ? null : String(out).trim();
    if (!g || !UUID_RE.test(g)) {
      out = newGuid();
      coerced = true;
      note = 'not a GUID — replaced with a new one';
    } else {
      out = g;
    }
    return { value: out, truncated: truncated, coerced: coerced, note: note };
  }

  // SAFE_NUMERIC — ISNULL(TRY_CAST(NULLIF(TRIM(src),'') AS <type>), 0)
  if (t === 'SAFE_NUMERIC') {
    var n = isBlank(out) ? null : numericOf(out);
    if (n === null) {
      out = 0;
      coerced = true;
      note = 'not a number — replaced with 0';
    } else {
      out = n;
    }
    return { value: out, truncated: truncated, coerced: coerced, note: note };
  }

  // SAFE_TRUNC — LEFT(src, <tgtLen>), with ISNULL(...,'') when the target
  // rejects nulls. Explicitly asked for: shorten to fit.
  if (t === 'SAFE_TRUNC') {
    var lenS = typeLength(tgtType);
    if (out == null) {
      if (o.tgtNotNull) out = '';
      return { value: out, truncated: false, coerced: false, note: null };
    }
    var sS = String(out);
    if (lenS != null && sS.length > lenS) {
      out = sS.slice(0, lenS);
      truncated = true;
      note = 'truncated to ' + lenS + ' characters';
    } else {
      out = sS;
    }
    return { value: out, truncated: truncated, coerced: coerced, note: note };
  }

  // CAST — the bug. `CAST(x AS NVARCHAR(8))` truncates silently in SQL Server;
  // sending the untouched literal instead raises msg 8152. For a character
  // target with a declared length, do what the cast does.
  //
  // Non-character targets keep the old behaviour: the literal goes down and
  // SQL Server converts it, which is correct for N'123' into an INT and is
  // what every existing map relies on.
  if (t === 'CAST' && out != null && isCharType(tgtType)) {
    var lenC = typeLength(tgtType);
    var sC = String(out);
    if (lenC != null && sC.length > lenC) {
      out = sC.slice(0, lenC);
      truncated = true;
      note = 'cast to ' + tgtType + ' — truncated to ' + lenC + ' characters';
    }
    return { value: out, truncated: truncated, coerced: coerced, note: note };
  }

  // NONE and anything else: the long-standing width rule. When the DECLARED
  // source width exceeds the DECLARED target width, the editor's generator
  // emits LEFT(src, n) and records a truncation warning. The runner has always
  // meant to match that — it just never had the types to do it with.
  //
  // Note this is a SCHEMA-level statement (NVARCHAR(50) into NVARCHAR(8)),
  // not "this value happens to be long". A value that overflows a target no
  // narrower than its source still fails, loudly, as it should: nobody asked
  // for it to be shortened.
  if (out != null && isCharType(srcType) && isCharType(tgtType)) {
    var lenT = typeLength(tgtType);
    var lenSrc = typeLength(srcType);
    if (lenT != null && (lenSrc == null || lenSrc > lenT)) {
      var sN = String(out);
      if (sN.length > lenT) {
        out = sN.slice(0, lenT);
        truncated = true;
        note = 'source is wider than the target — truncated to ' + lenT + ' characters';
      }
    }
  }

  return { value: out, truncated: truncated, coerced: coerced, note: note };
}

/* RFC4122 v4, from Math.random. Not cryptographic — it stands in for
   SQL Server's NEWID() on a row the source could not supply a GUID for, and
   uniqueness is all that is being asked of it. */
function newGuid() {
  var s = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx';
  return s.replace(/[xy]/g, function (c) {
    var r = Math.random() * 16 | 0;
    var val = c === 'x' ? r : ((r & 0x3) | 0x8);
    return val.toString(16);
  });
}

/**
 * Would this mapping still overflow its target after the transform runs?
 * Preflight's question. Returns null when the value fits or is handled, or a
 * short reason when it does not.
 */
function txResidualRisk(m, v, opts) {
  var o = opts || {};
  var res = txApply(m, v, o);
  if (res.value == null) return null;
  var tgtType = o.tgtType || (m && m.tgtType) || '';
  if (isCharType(tgtType)) {
    var len = typeLength(tgtType);
    if (len != null && String(res.value).length > len) return 'still longer than ' + tgtType;
  }
  return null;
}

return {
  txApply: txApply,
  txResidualRisk: txResidualRisk,
  typeLength: typeLength,
  isCharType: isCharType,
  isNumericType: isNumericType,
  isGuidType: isGuidType,
  isDateType: isDateType,
};
});
