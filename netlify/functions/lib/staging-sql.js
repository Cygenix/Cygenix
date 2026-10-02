/* ============================================================================
   lib/staging-sql.js — may this statement write here? The Dev Console's
   staging-schema rule, as a reader of SQL rather than a list of hopes
   ----------------------------------------------------------------------------
   Oct-2026. A Dev Console session can be opened with a STAGING SCHEMA: a
   schema in the connected database (usually the source) where Claude builds
   the staging tables a Conversion Template describes and loads them from the
   rest of the database. The rule the owner agreed to is short:

       inside the staging schema, anything — create, load, empty, drop;
       everywhere else, read only.

   The bridge (cc-mcp.js) decides "read or write" with the product's own
   classifier. A read runs inside a transaction that is always rolled back,
   so a read that was secretly a write keeps nothing. A WRITE comes here, and
   runs for real only if this file can show that every object it writes to
   is a table in the staging schema (or a #temp table or a @table variable,
   which die with the call).

   HOW IT DECIDES
   It tokenises the statement the way the server will — strings, quoted
   identifiers, brackets — and then looks at EVERY word that can write,
   wherever it sits: INSERT, INTO, UPDATE, DELETE, CREATE, DROP, ALTER,
   TRUNCATE, SET IDENTITY_INSERT. Each one must be followed by a name of the
   form <staging>.<table>, two parts exactly — not one (that is the default
   schema), not three (that could be another database's schema of the same
   name). Words that can write in ways a name cannot show — EXEC, MERGE,
   GRANT, BULK, OPENQUERY, a FOREIGN KEY's REFERENCES, CASCADE, SWITCH and
   the rest of the list below — are refused outright in a staging write.
   Anything this file does not recognise is a refusal, not a pass: the cost
   of a false refusal is Claude rewriting one statement; the cost of a false
   pass is somebody's source database.

   WHAT IT REFUSES THAT IS PERFECTLY GOOD SQL, AND WHY
     · Comments, in a write. SQL Server and PostgreSQL both nest block comments, and a
       reader that ends a comment one token early can be walked into reading
       code as a string. Refusing comments in writes removes the question.
     · In PostgreSQL, backslashes and dollar quotes in a write — E'' strings
       and $$ bodies are the two places a quote does not mean what it looks
       like it means.
     · An UPDATE or DELETE whose target is an alias (UPDATE s SET … FROM
       staging.t s). Write UPDATE staging.t SET … FROM staging.t JOIN …
       instead; the alias form cannot be checked without a full parser.
     · Statements that are not a statement this file knows how to start
       (SQL Server runs a bare procedure name as the first statement of a
       batch, and a procedure can do anything).

   WHAT IT CANNOT SEE — written down so nobody assumes otherwise
     · An object that already exists IN the staging schema and points
       elsewhere — a view, a synonym, a trigger somebody made by hand. This
       file never lets Claude create one, but it cannot vouch for what was
       there first. The staging schema should be new, or Cygenix's own.
     · A PostgreSQL function, called from a SELECT, that writes. SQL Server
       functions cannot change data; PostgreSQL's can. The well-known ones
       are refused by name; a user-defined one is not detectable from text.

   No I/O, no dependencies: cc-mcp.js and tests/staging-sql.test.js are its
   only callers.
   ========================================================================== */
'use strict';

/* ── The staging schema's own name ───────────────────────────────────────
   A plain identifier, and never one of the schemas a database is built on.
   The Function App validates the same way when a session is opened
   (azure-function/src/claude-code.js, stagingSchemaProblem); the test keeps
   the two agreeing. */
const SCHEMA_RE = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
const RESERVED_SCHEMAS = ['dbo', 'sys', 'guest', 'information_schema', 'public', 'pg_catalog', 'pg_toast', 'pg_temp'];
function stagingSchemaProblem(name, dbType) {
  const s = String(name == null ? '' : name);
  if (!s) return 'Give the staging schema a name.';
  if (!SCHEMA_RE.test(s)) return 'A staging schema name is letters, digits and underscores, starting with a letter, up to 63 characters.';
  const low = s.toLowerCase();
  if (RESERVED_SCHEMAS.indexOf(low) !== -1 || /^db_/.test(low) || /^pg_/.test(low)) {
    return '"' + s + '" is one of the database\'s own schemas. Choose a schema of its own for staging, such as "staging".';
  }
  if (dbType === 'postgres' && s !== low) return 'In PostgreSQL, use a lower-case staging schema name ("' + low + '"), so that it means the same thing quoted or not.';
  return '';
}

/* ── Tokens ───────────────────────────────────────────────────────────── */
const WORD_START = /[\p{L}_]/u;
const WORD_PART = /[\p{L}\p{N}_$#@]/u;

function tokenize(sql, dialect) {
  const pg = dialect === 'postgres';
  const s = String(sql);
  const out = [];
  let i = 0;
  const n = s.length;
  while (i < n) {
    const c = s[i];
    if (/\s/.test(c)) { i++; continue; }
    if (c === "'" || ((c === 'N' || c === 'n') && s[i + 1] === "'" && !pg)) {
      let j = c === "'" ? i + 1 : i + 2;
      for (;;) {
        if (j >= n) return { error: 'an unterminated string' };
        if (s[j] === "'") { if (s[j + 1] === "'") { j += 2; continue; } break; }
        j++;
      }
      out.push({ t: 'str', v: s.slice(i, j + 1) });
      i = j + 1; continue;
    }
    if (c === '[' && !pg) {
      let j = i + 1, v = '';
      for (;;) {
        if (j >= n) return { error: 'an unterminated [name]' };
        if (s[j] === ']') { if (s[j + 1] === ']') { v += ']'; j += 2; continue; } break; }
        v += s[j]; j++;
      }
      out.push({ t: 'qid', v });
      i = j + 1; continue;
    }
    if (c === '"') {
      let j = i + 1, v = '';
      for (;;) {
        if (j >= n) return { error: 'an unterminated "name"' };
        if (s[j] === '"') { if (s[j + 1] === '"') { v += '"'; j += 2; continue; } break; }
        v += s[j]; j++;
      }
      out.push({ t: 'qid', v });
      i = j + 1; continue;
    }
    if (c === '@' || c === '#') {
      let j = i + 1;
      while (j < n && (s[j] === c || WORD_PART.test(s[j]))) j++;
      out.push({ t: c === '@' ? 'var' : 'temp', v: s.slice(i, j) });
      i = j; continue;
    }
    if (WORD_START.test(c)) {
      let j = i + 1;
      while (j < n && WORD_PART.test(s[j])) j++;
      const v = s.slice(i, j);
      out.push({ t: 'word', v, up: v.toUpperCase() });
      i = j; continue;
    }
    if (/[0-9]/.test(c)) {
      let j = i + 1;
      while (j < n && /[0-9.eE]/.test(s[j])) j++;
      out.push({ t: 'num', v: s.slice(i, j) });
      i = j; continue;
    }
    out.push({ t: 'op', v: c });
    i++;
  }
  return { tokens: out };
}

/* ── The vocabulary ───────────────────────────────────────────────────── */
// A staging write may begin with one of these, and nothing else.
const FIRST_WORDS = ['SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE',
  'IF', 'BEGIN', 'DECLARE', 'SET'];
// Words that change things a name after them cannot show, or that leave the
// one database: never part of a staging write. In both dialects, a foreign
// key's REFERENCES (a staging table must not reach into a real one) and
// CASCADE (which follows dependencies out of the schema).
const NEVER_ALL = ['CASCADE', 'REFERENCES'];
// SQL Server. Every word here is RESERVED in T-SQL, so it cannot be an
// unquoted column name and a refusal never catches an innocent SELECT list —
// except RECEIVE and CONVERSATION, which are Service Broker's and rare
// enough as column names to be worth the occasional false refusal.
const NEVER_MSSQL = ['MERGE', 'EXEC', 'EXECUTE', 'GRANT', 'REVOKE', 'DENY', 'DBCC', 'BACKUP', 'RESTORE', 'BULK',
  'OPENROWSET', 'OPENQUERY', 'OPENDATASOURCE', 'KILL', 'SHUTDOWN', 'RECONFIGURE', 'CHECKPOINT', 'WRITETEXT',
  'UPDATETEXT', 'SETUSER', 'REVERT', 'USE', 'LOAD', 'DUMP', 'RECEIVE', 'CONVERSATION'];
// SQL Server words that are NOT reserved — Switch, Enable, Send, Signature
// are all reasonable column names — so they are refused only where they act:
// ALTER TABLE … SWITCH (moves rows to another table), ENABLE/DISABLE TRIGGER,
// SEND ON CONVERSATION, BEGIN DIALOG, ADD [COUNTER] SIGNATURE, ADD/DROP
// SENSITIVITY CLASSIFICATION. PostgreSQL needs no list of this kind: its
// statements are separated by ';', so the first word of every one of them
// is held to FIRST_WORDS, and COPY, CALL, DO, VACUUM, COMMENT, GRANT and the
// rest never get that far.
const NEVER = NEVER_ALL.concat(NEVER_MSSQL);
// PostgreSQL functions that change something outside the statement's own
// tables. A user-defined function is not detectable; these are.
const PG_SIDE_EFFECTS = ['DBLINK', 'DBLINK_EXEC', 'DBLINK_CONNECT', 'SETVAL', 'LO_UNLINK', 'LO_IMPORT', 'LO_EXPORT',
  'PG_TERMINATE_BACKEND', 'PG_CANCEL_BACKEND', 'PG_RELOAD_CONF', 'PG_ROTATE_LOGFILE', 'SET_CONFIG',
  'PG_FILE_WRITE', 'PG_ADVISORY_LOCK', 'PG_ADVISORY_XACT_LOCK'];

/* ── Names ────────────────────────────────────────────────────────────── */
function isIdent(tok) { return !!tok && (tok.t === 'word' || tok.t === 'qid'); }

// A dotted name starting at tokens[k]: its parts and where it ends. An
// empty part (db..t) or a trailing dot is no name.
function readName(toks, k) {
  const parts = [];
  if (!isIdent(toks[k])) return null;
  parts.push(toks[k]);
  let j = k + 1;
  while (toks[j] && toks[j].t === 'op' && toks[j].v === '.') {
    if (!isIdent(toks[j + 1])) return null;
    parts.push(toks[j + 1]);
    j += 2;
  }
  return { parts, end: j };
}
function sameName(tok, schema, dialect) {
  if (!tok) return false;
  if (dialect === 'postgres') return tok.t === 'qid' ? tok.v === schema : tok.v.toLowerCase() === schema.toLowerCase();
  return tok.v.toLowerCase() === schema.toLowerCase();
}
function show(name) { return name.parts.map(p => p.v).join('.'); }

/* ── The check ────────────────────────────────────────────────────────── */
function checkStagingWrite(sql, opts) {
  const o = opts || {};
  const schema = String(o.schema || '');
  const dialect = o.dialect === 'postgres' ? 'postgres' : 'sqlserver';
  const refuse = (why) => ({ ok: false, why });
  if (stagingSchemaProblem(schema, dialect)) return refuse('This session has no valid staging schema.');
  const text = String(sql == null ? '' : sql);
  if (/--|\/\*/.test(text)) return refuse('Comments are not accepted in a statement that changes the staging schema. Send it without -- or /* */ comments.');
  if (dialect === 'postgres' && /[\\$]/.test(text)) return refuse('Backslashes and dollar quotes are not accepted in a PostgreSQL statement that changes the staging schema.');

  const tz = tokenize(text, dialect);
  if (tz.error) return refuse('The statement has ' + tz.error + '.');
  const toks = tz.tokens;
  if (!toks.length) return refuse('There is no statement.');

  const writes = [];
  const where = (name) => '"' + show(name) + '"';
  const outside = (name, op) => refuse(op + ' ' + where(name) + ' is outside the staging schema "' + schema
    + '". In this session only tables in "' + schema + '" can be changed (write the name as ' + schema + '.<table>); everything else is read-only.');
  // A target a write may land on: <staging>.<table>, a #temp table, a @table variable.
  const target = (k, op) => {
    const tk = toks[k];
    if (tk && tk.t === 'temp') {
      if (dialect === 'postgres' || /^##/.test(tk.v)) return { bad: refuse(op + ' ' + tk.v + ' is not a staging table.') };
      writes.push({ op, object: tk.v }); return { end: k + 1 };
    }
    if (tk && tk.t === 'var' && !/^@@/.test(tk.v)) { writes.push({ op, object: tk.v }); return { end: k + 1 }; }
    const name = readName(toks, k);
    if (!name) return { bad: refuse(op + ' must be followed by a table name, written as ' + schema + '.<table>.') };
    if (name.parts.length === 1 && name.parts[0].t === 'word') {
      return { bad: refuse(op + ' "' + show(name) + '": name the table in full, as ' + schema + '.<table>. A table without its schema, or an alias, '
        + 'cannot be checked — for an UPDATE or DELETE with a join, write UPDATE ' + schema + '.<table> SET … FROM ' + schema + '.<table> JOIN ….') };
    }
    if (name.parts.length !== 2 || !sameName(name.parts[0], schema, dialect)) return { bad: outside(name, op) };
    writes.push({ op, object: show(name) });
    return { end: name.end };
  };
  // Skip TOP (n) [PERCENT] after INSERT/UPDATE/DELETE.
  const skipTop = (k) => {
    if (toks[k] && toks[k].up === 'TOP' && toks[k + 1] && toks[k + 1].v === '(') {
      let depth = 0, j = k + 1;
      for (; j < toks.length; j++) {
        if (toks[j].v === '(') depth++;
        else if (toks[j].v === ')') { depth--; if (depth === 0) break; }
      }
      j++;
      if (toks[j] && toks[j].up === 'PERCENT') j++;
      return j;
    }
    return k;
  };
  const optional = (k, ...words) => { while (toks[k] && words.indexOf(toks[k].up) !== -1) k++; return k; };
  const nextIs = (k, ...words) => toks[k] && words.indexOf(toks[k].up) !== -1;

  let first = 0;
  while (toks[first] && toks[first].t === 'op' && toks[first].v === ';') first++;     // ;WITH …
  if (!toks[first]) return refuse('There is no statement.');
  if (toks[first].t !== 'word' || FIRST_WORDS.indexOf(toks[first].up) === -1) {
    if (!(toks[first].t === 'op' && toks[first].v === '(')) {
      return refuse('A statement that changes the staging schema must start with INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE, SELECT, WITH, IF, BEGIN, DECLARE or SET.');
    }
  }
  // PostgreSQL separates statements with ';', so each one's first word can
  // be held to the same list. (SQL Server cannot be split that way — a
  // statement may follow another with no ';' — which is why every writing
  // word is checked wherever it is.)
  if (dialect === 'postgres') {
    for (let k = 1; k < toks.length; k++) {
      if (toks[k - 1].t === 'op' && toks[k - 1].v === ';' && toks[k].t === 'word' && FIRST_WORDS.indexOf(toks[k].up) === -1) {
        return refuse('"' + toks[k].v + '" cannot be part of a statement that changes the staging schema.');
      }
    }
  }

  const done = new Set();
  for (let k = 0; k < toks.length; k++) {
    const tk = toks[k];
    if (tk.t !== 'word' || done.has(k)) continue;
    const prev = toks[k - 1];
    const w = tk.up;
    if (NEVER_ALL.indexOf(w) !== -1 || (dialect !== 'postgres' && NEVER_MSSQL.indexOf(w) !== -1)) {
      return refuse(w + ' is not allowed in a statement that changes the staging schema.');
    }
    if (dialect !== 'postgres') {
      const acts = (w === 'ENABLE' || w === 'DISABLE') ? nextIs(k + 1, 'TRIGGER')
        : w === 'SEND' ? nextIs(k + 1, 'ON')
        : w === 'DIALOG' ? !!(prev && prev.up === 'BEGIN')
        : (w === 'SIGNATURE' || w === 'SENSITIVITY') ? !!(prev && ['ADD', 'COUNTER', 'DROP'].indexOf(prev.up) !== -1)
        : false;
      if (acts) return refuse((prev && w !== 'ENABLE' && w !== 'DISABLE' && w !== 'SEND' ? prev.up + ' ' : '') + w
        + ' is not allowed in a statement that changes the staging schema.');
    }
    if (dialect === 'postgres' && PG_SIDE_EFFECTS.indexOf(w) !== -1) return refuse(tk.v + '() changes things outside the staging schema and is not allowed here.');

    switch (w) {
      case 'INSERT': {
        let j = skipTop(k + 1);
        if (nextIs(j, 'INTO')) { done.add(j); j++; }
        const r = target(j, 'INSERT INTO'); if (r.bad) return r.bad;
        break;
      }
      case 'INTO': {
        const r = target(k + 1, 'INTO'); if (r.bad) return r.bad;
        break;
      }
      case 'UPDATE': {
        if (prev && ['ON', 'DO', 'FOR'].indexOf(prev.up) !== -1) break;   // ON UPDATE, DO UPDATE, FOR UPDATE
        if (nextIs(k + 1, 'STATISTICS')) { const r = target(k + 2, 'UPDATE STATISTICS'); if (r.bad) return r.bad; break; }
        const r = target(optional(skipTop(k + 1), 'ONLY'), 'UPDATE'); if (r.bad) return r.bad;
        break;
      }
      case 'DELETE': {
        if (prev && prev.up === 'ON') break;                              // ON DELETE
        let j = skipTop(k + 1);
        if (nextIs(j, 'FROM')) j++;
        const r = target(optional(j, 'ONLY'), 'DELETE FROM'); if (r.bad) return r.bad;
        break;
      }
      case 'TRUNCATE': {
        let j = optional(k + 1, 'TABLE', 'ONLY');
        for (;;) {
          const r = target(j, 'TRUNCATE'); if (r.bad) return r.bad;
          j = r.end;
          if (!(toks[j] && toks[j].v === ',')) break;
          j++;
        }
        break;
      }
      case 'CREATE': {
        let j = k + 1;
        if (nextIs(j, 'SCHEMA')) {
          j = optional(j + 1);
          if (nextIs(j, 'IF') && nextIs(j + 1, 'NOT') && nextIs(j + 2, 'EXISTS')) j += 3;
          if (!isIdent(toks[j]) || !sameName(toks[j], schema, dialect) || (toks[j + 1] && toks[j + 1].v === '.')) {
            return refuse('Only the staging schema itself, "' + schema + '", can be created in this session.');
          }
          j++;
          if (nextIs(j, 'AUTHORIZATION') && isIdent(toks[j + 1])) j += 2;
          while (toks[j] && toks[j].v === ';') j++;
          if (j < toks.length) return refuse('CREATE SCHEMA must be sent on its own, with nothing after it.');
          writes.push({ op: 'CREATE SCHEMA', object: schema });
          k = j; break;
        }
        if (dialect === 'postgres' && nextIs(j, 'TEMP', 'TEMPORARY', 'UNLOGGED')) {
          if (nextIs(j, 'UNLOGGED')) j++;
          else return refuse('Create a staging table in "' + schema + '" rather than a temporary table.');
        }
        if (nextIs(j, 'TABLE')) {
          j++;
          if (nextIs(j, 'IF') && nextIs(j + 1, 'NOT') && nextIs(j + 2, 'EXISTS')) j += 3;
          const r = target(j, 'CREATE TABLE'); if (r.bad) return r.bad;
          break;
        }
        j = optional(j, 'UNIQUE', 'CLUSTERED', 'NONCLUSTERED');
        if (nextIs(j, 'INDEX')) {
          j++;
          if (dialect === 'postgres' && nextIs(j, 'IF') && nextIs(j + 1, 'NOT') && nextIs(j + 2, 'EXISTS')) j += 3;
          if (!isIdent(toks[j])) return refuse('CREATE INDEX needs an index name.');
          j++;
          if (!nextIs(j, 'ON')) return refuse('CREATE INDEX must name its table with ON ' + schema + '.<table>.');
          const r = target(j + 1, 'CREATE INDEX ON'); if (r.bad) return r.bad;
          break;
        }
        return refuse('Only CREATE SCHEMA ' + schema + ', CREATE TABLE ' + schema + '.<table> and CREATE INDEX … ON '
          + schema + '.<table> are allowed in this session.');
      }
      case 'DROP': {
        let j = k + 1;
        const ifExists = (x) => (nextIs(x, 'IF') && nextIs(x + 1, 'EXISTS')) ? x + 2 : x;
        if (nextIs(j, 'TABLE')) {
          j = ifExists(j + 1);
          for (;;) {
            const r = target(j, 'DROP TABLE'); if (r.bad) return r.bad;
            j = r.end;
            if (!(toks[j] && toks[j].v === ',')) break;
            j++;
          }
          break;
        }
        if (nextIs(j, 'SCHEMA')) {
          j = ifExists(j + 1);
          if (!isIdent(toks[j]) || !sameName(toks[j], schema, dialect) || (toks[j + 1] && /^[.,]$/.test(toks[j + 1].v))) {
            return refuse('Only the staging schema itself, "' + schema + '", can be dropped in this session.');
          }
          writes.push({ op: 'DROP SCHEMA', object: schema });
          break;
        }
        if (nextIs(j, 'INDEX')) {
          j = ifExists(j + 1);
          const name = readName(toks, j);
          if (!name) return refuse('DROP INDEX needs an index name.');
          if (nextIs(name.end, 'ON')) {
            if (name.parts.length !== 1) return refuse('Write DROP INDEX <index> ON ' + schema + '.<table>.');
            const r = target(name.end + 1, 'DROP INDEX ON'); if (r.bad) return r.bad;
          } else if ((dialect === 'postgres' ? name.parts.length !== 2 : name.parts.length !== 3) || !sameName(name.parts[0], schema, dialect)) {
            // SQL Server reads DROP INDEX a.b as table a (in the DEFAULT
            // schema) and index b, so only the three-part form, or ON, names
            // a staging table there.
            return refuse('DROP INDEX "' + show(name) + '" does not name a staging table. Write DROP INDEX <index> ON ' + schema + '.<table>.');
          } else {
            writes.push({ op: 'DROP INDEX', object: show(name) });
          }
          break;
        }
        return refuse('Only tables, indexes and the staging schema itself can be dropped in this session.');
      }
      case 'ALTER': {
        if (!nextIs(k + 1, 'TABLE')) return refuse('Only ALTER TABLE ' + schema + '.<table> is allowed in this session.');
        let j = k + 2;
        if (dialect === 'postgres') {
          if (nextIs(j, 'IF') && nextIs(j + 1, 'EXISTS')) j += 2;
          j = optional(j, 'ONLY');
        }
        const r = target(j, 'ALTER TABLE'); if (r.bad) return r.bad;
        // SWITCH moves a table's rows into another table, SET SCHEMA moves the
        // table itself, and RENAME is refused with them rather than reasoned
        // about. Scanned to the end of the batch, not the statement: SQL Server
        // does not need a ';' between statements.
        for (let x = r.end; x < toks.length; x++) {
          if (toks[x].up === 'SWITCH' || toks[x].up === 'RENAME' || (toks[x].up === 'SET' && nextIs(x + 1, 'SCHEMA'))) {
            return refuse('A staging table cannot be switched, renamed or moved to another schema here.');
          }
        }
        break;
      }
      case 'SET': {
        if (nextIs(k + 1, 'IDENTITY_INSERT')) { const r = target(k + 2, 'SET IDENTITY_INSERT'); if (r.bad) return r.bad; break; }
        if (nextIs(k + 1, 'ROLE', 'SESSION', 'SEARCH_PATH')) return refuse('SET ' + toks[k + 1].v.toUpperCase() + ' is not allowed in this session.');
        break;
      }
      default: break;
    }
  }
  return { ok: true, writes };
}

module.exports = { checkStagingWrite, stagingSchemaProblem, tokenize, SCHEMA_RE, RESERVED_SCHEMAS, NEVER, NEVER_ALL, NEVER_MSSQL, FIRST_WORDS };
