/* staging-sql.test.js — the Dev Console's staging-schema rule, from the outside
   ---------------------------------------------------------------------------
   Plain Node, no framework. netlify/functions/lib/staging-sql.js decides
   whether a statement that WRITES may run in a staging session: only if
   every object it writes to is <staging>.<table>. This file is mostly the
   ways somebody — or a model being clever — might try to write elsewhere,
   each of which must be refused, and the ordinary staging work that must
   still go through. A false refusal costs one rewritten statement; a false
   pass costs a source database. The cases are weighted accordingly. */
'use strict';

const path = require('path');
const S = require(path.join(__dirname, '..', 'netlify', 'functions', 'lib', 'staging-sql.js'));

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra !== undefined ? '  → ' + String(extra).slice(0, 400) : '')); }
};
const section = (t) => console.log('\n' + t);
const run = (sql, dialect, schema) => S.checkStagingWrite(sql, { schema: schema || 'staging', dialect: dialect || 'sqlserver' });
const allows = (label, sql, dialect, schema) => { const r = run(sql, dialect, schema); check('allows: ' + label, r.ok === true, r.why); return r; };
const refuses = (label, sql, dialect, re) => {
  const r = run(sql, dialect);
  check('REFUSES: ' + label, r.ok === false && (!re || re.test(r.why)), r.ok ? 'it passed' : r.why);
  return r;
};

console.log('Staging schema — what may be written, and where\n');

section('1. The schema name');
{
  check('a plain name is fine', S.stagingSchemaProblem('staging') === '' && S.stagingSchemaProblem('Stage_01') === '');
  check('empty is refused', /Give the staging schema a name/.test(S.stagingSchemaProblem('')));
  check('punctuation, spaces and a leading digit are refused',
    ['stg.x', 'my stage', '1stage', 'st[a]ge', 'a-b', "x'y"].every(n => S.stagingSchemaProblem(n) !== ''));
  check('the database\'s own schemas are refused, in any case',
    ['dbo', 'DBO', 'sys', 'guest', 'INFORMATION_SCHEMA', 'public', 'pg_catalog', 'db_owner', 'db_datareader', 'pg_anything']
      .every(n => /own schemas/.test(S.stagingSchemaProblem(n))));
  check('64 characters is too long, 63 is fine', S.stagingSchemaProblem('a'.repeat(64)) !== '' && S.stagingSchemaProblem('a'.repeat(63)) === '');
  check('PostgreSQL wants lower case, so quoted and unquoted agree', /lower-case/.test(S.stagingSchemaProblem('Staging', 'postgres'))
    && S.stagingSchemaProblem('staging', 'postgres') === '' && S.stagingSchemaProblem('Staging', 'sqlserver') === '');
  check('a session with no valid schema refuses every write', run('INSERT INTO staging.t SELECT 1', 'sqlserver', 'dbo').ok === false);
}

section('2. The ordinary work of building staging — SQL Server');
{
  allows('CREATE SCHEMA, alone', 'CREATE SCHEMA staging');
  allows('CREATE SCHEMA in brackets, with AUTHORIZATION', 'CREATE SCHEMA [staging] AUTHORIZATION dbo;');
  allows('a CREATE TABLE in the template\'s own form',
    "IF OBJECT_ID(N'staging.Matter', N'U') IS NULL\nBEGIN\n    CREATE TABLE [staging].[Matter] (\n        [MattIndex] int NULL,\n        [Number] nvarchar(64) NULL\n    );\nEND");
  allows('several CREATE TABLEs in one batch', 'CREATE TABLE staging.a (x int NULL); CREATE TABLE staging.b (y nvarchar(max) NULL)');
  const ins = allows('INSERT … SELECT from the real tables, columns named Comment, Switch, Enable, Lock, Copy',
    'INSERT INTO staging.Matter (Number, Comment, Switch, Enable, Lock, Copy) SELECT m.Num, m.Comment, m.Switch, m.Enable, m.Lock, m.Copy FROM dbo.Matters m JOIN dbo.Clients c ON c.Id = m.ClientId WHERE m.Open = 1');
  check('…and says what it writes', ins.writes && ins.writes.length === 1 && ins.writes[0].op === 'INSERT INTO' && ins.writes[0].object === 'staging.Matter', JSON.stringify(ins.writes));
  allows('INSERT without INTO, and INSERT TOP (n)', 'INSERT staging.t (a) SELECT a FROM dbo.x; INSERT TOP (100) INTO staging.t (a) SELECT a FROM dbo.x');
  allows('a CTE before the INSERT, and a leading ;', ';WITH src AS (SELECT a FROM dbo.x) INSERT INTO staging.t (a) SELECT a FROM src');
  allows('SELECT … INTO a new staging table', 'SELECT a, b INTO staging.t FROM dbo.x WHERE a > 0');
  allows('TRUNCATE a staging table', 'TRUNCATE TABLE staging.t');
  allows('DROP TABLE IF EXISTS, a list of them', 'DROP TABLE IF EXISTS staging.a, staging.b');
  allows('the guarded drop', "IF OBJECT_ID(N'staging.t', N'U') IS NOT NULL DROP TABLE staging.t");
  allows('DELETE with and without WHERE', 'DELETE FROM staging.t WHERE a IS NULL; DELETE staging.t');
  allows('UPDATE with a join, written the checkable way', 'UPDATE staging.t SET b = x.b FROM staging.t JOIN dbo.x ON x.a = staging.t.a');
  allows('ALTER TABLE ADD a column', 'ALTER TABLE staging.t ADD c int NULL');
  allows('CREATE and DROP an index', 'CREATE NONCLUSTERED INDEX ix_t_a ON staging.t (a); DROP INDEX ix_t_a ON staging.t');
  allows('the three-part DROP INDEX', 'DROP INDEX staging.t.ix_t_a');
  allows('SET IDENTITY_INSERT on a staging table', 'SET IDENTITY_INSERT staging.t ON; INSERT INTO staging.t (id) VALUES (1); SET IDENTITY_INSERT staging.t OFF');
  allows('a #temp table and a @table variable', 'SELECT a INTO #k FROM dbo.x; DECLARE @t TABLE (a int); INSERT INTO @t SELECT a FROM #k; INSERT INTO staging.t SELECT a FROM @t');
  allows('a transaction and TRY/CATCH', 'BEGIN TRY BEGIN TRANSACTION; INSERT INTO staging.t SELECT 1; COMMIT; END TRY BEGIN CATCH ROLLBACK; THROW; END CATCH');
  allows('DROP SCHEMA staging', 'DROP SCHEMA staging');
  allows('the schema name in any case', 'insert into STAGING.T select 1');
  allows('another staging name', 'INSERT INTO conv.t SELECT 1', 'sqlserver', 'conv');
  allows('strings that LOOK like writes are only strings', "INSERT INTO staging.t (note) VALUES (N'DROP TABLE dbo.x; UPDATE dbo.y SET a = 1'), ('it''s fine')");
  allows('a name in brackets with a ]] inside', 'INSERT INTO [staging].[odd]]name] SELECT 1');
}

section('3. Writing anywhere else — SQL Server');
{
  refuses('UPDATE a real table', 'UPDATE dbo.Matters SET Open = 0 WHERE Id = 1', null, /outside the staging schema "staging"/);
  refuses('DELETE from a real table', 'DELETE FROM dbo.Matters WHERE Id = 1');
  refuses('INSERT into a real table', 'INSERT INTO dbo.Matters (Id) VALUES (1)');
  refuses('a table with no schema (the default schema)', 'INSERT INTO Matters (Id) VALUES (1)', null, /name the table in full/);
  refuses('a write hidden after a staging write with no ;', 'INSERT INTO staging.t SELECT 1 UPDATE dbo.x SET a = 1 WHERE a = 2');
  refuses('a write hidden after a ;', 'INSERT INTO staging.t SELECT 1; DELETE FROM dbo.x WHERE a = 1');
  refuses('a write hidden inside a TRY block', 'BEGIN TRY INSERT INTO staging.t SELECT 1; TRUNCATE TABLE dbo.x END TRY BEGIN CATCH END CATCH');
  refuses('three-part name: another database\'s staging schema', 'INSERT INTO OtherDb.staging.t SELECT 1');
  refuses('four-part name: a linked server', 'INSERT INTO Srv.Db.staging.t SELECT 1');
  refuses('db..table', 'INSERT INTO Db..t SELECT 1');
  refuses('a schema that only STARTS with the staging name', 'INSERT INTO staging2.t SELECT 1');
  refuses('a dbo table named like the schema', 'INSERT INTO dbo.staging SELECT 1');
  refuses('the alias form of UPDATE', 'UPDATE s SET a = 1 FROM staging.t s', null, /name the table in full/);
  refuses('the alias form of DELETE', 'DELETE s FROM staging.t s');
  refuses('OUTPUT … INTO a real table', 'DELETE FROM staging.t OUTPUT deleted.a INTO dbo.Audit (a)');
  refuses('SELECT … INTO a real table', 'SELECT * INTO dbo.Copy FROM staging.t');
  refuses('a global ##temp table', 'SELECT a INTO ##shared FROM dbo.x');
  refuses('TRUNCATE a real table', 'TRUNCATE TABLE dbo.x');
  refuses('TRUNCATE a list that strays', 'TRUNCATE TABLE staging.a, dbo.b');
  refuses('DROP TABLE a real table', 'DROP TABLE dbo.x');
  refuses('DROP TABLE a list that strays', 'DROP TABLE staging.a, dbo.b');
  refuses('DROP another schema', 'DROP SCHEMA dbo');
  refuses('DROP a view or a procedure', 'DROP VIEW staging.v; DROP PROCEDURE dbo.p');
  refuses('DROP INDEX a.b (SQL Server reads a as a DEFAULT-schema table)', 'DROP INDEX staging.ix', null, /does not name a staging table/);
  refuses('DROP INDEX on a real table', 'DROP INDEX ix ON dbo.x');
  refuses('CREATE TABLE elsewhere', 'CREATE TABLE dbo.t (a int)');
  refuses('CREATE a view, a synonym, a procedure, a trigger', 'CREATE VIEW staging.v AS SELECT 1', null, /Only CREATE SCHEMA/);
  refuses('…a synonym', 'CREATE SYNONYM staging.s FOR dbo.x');
  refuses('…a trigger', 'CREATE TRIGGER staging.tr ON staging.t AFTER INSERT AS DELETE FROM dbo.x');
  refuses('CREATE SCHEMA with something else inside it', 'CREATE SCHEMA staging CREATE TABLE t (a int) GRANT SELECT ON t TO public', null, /on its own/);
  refuses('CREATE another schema', 'CREATE SCHEMA other');
  refuses('ALTER SCHEMA … TRANSFER a real table in', 'ALTER SCHEMA staging TRANSFER dbo.Matters', null, /Only ALTER TABLE/);
  refuses('ALTER TABLE … SWITCH rows out to a real table', 'ALTER TABLE staging.t SWITCH TO dbo.x', null, /switched/);
  refuses('ALTER a real table', 'ALTER TABLE dbo.x ADD c int');
  refuses('a foreign key into a real table', 'CREATE TABLE staging.t (a int REFERENCES dbo.x (a))', null, /REFERENCES/);
  refuses('ON DELETE CASCADE', 'ALTER TABLE staging.t ADD CONSTRAINT f FOREIGN KEY (a) REFERENCES staging.u (a) ON DELETE CASCADE');
  refuses('EXEC a procedure', "INSERT INTO staging.t EXEC dbo.GetRows", null, /EXEC/);
  refuses('EXEC a string', "EXEC('DELETE FROM dbo.x')");
  refuses('a bare procedure name as the first statement', "sp_rename 'dbo.x', 'y'", null, /must start with/);
  refuses('MERGE, even into staging', 'MERGE INTO staging.t USING dbo.x ON 1 = 1 WHEN NOT MATCHED THEN INSERT (a) VALUES (1);', null, /MERGE|must start with/);
  refuses('MERGE after another statement', 'INSERT INTO staging.t SELECT 1; MERGE INTO staging.t USING dbo.x ON 1 = 1 WHEN MATCHED THEN DELETE;', null, /MERGE/);
  refuses('OPENQUERY to a linked server', "INSERT INTO staging.t SELECT * FROM OPENQUERY(Srv, 'SELECT 1')");
  refuses('OPENROWSET', "INSERT INTO staging.t SELECT * FROM OPENROWSET(BULK 'C:\\x', SINGLE_CLOB) AS x");
  refuses('BULK INSERT', "BULK INSERT staging.t FROM 'C:\\data.csv'");
  refuses('GRANT', 'GRANT SELECT ON staging.t TO public');
  refuses('USE another database', 'USE master; INSERT INTO staging.t SELECT 1', null, /must start with|USE/);
  refuses('DBCC', 'DBCC CHECKIDENT (\'staging.t\', RESEED, 0)');
  refuses('DISABLE TRIGGER on a real table', 'DISABLE TRIGGER trg ON dbo.x; INSERT INTO staging.t SELECT 1');
  refuses('SEND ON CONVERSATION', 'INSERT INTO staging.t SELECT 1; SEND ON CONVERSATION @h (N\'x\')');
  refuses('ADD SIGNATURE', 'INSERT INTO staging.t SELECT 1; ADD SIGNATURE TO dbo.p BY CERTIFICATE c');
  refuses('WRITETEXT / UPDATETEXT', 'INSERT INTO staging.t SELECT 1; WRITETEXT dbo.x.c @p N\'x\'');
  refuses('UPDATE STATISTICS on a real table', 'UPDATE STATISTICS dbo.x');
  refuses('SET IDENTITY_INSERT on a real table', 'SET IDENTITY_INSERT dbo.x ON');
}

section('4. Things the lexer must not be walked round');
{
  refuses('a line comment', 'INSERT INTO staging.t SELECT 1 -- note', null, /Comments are not accepted/);
  refuses('a block comment', 'INSERT INTO staging.t /* x */ SELECT 1', null, /Comments/);
  refuses('a nested comment hiding a write from a non-nesting reader', "/* /* */ ' */ UPDATE dbo.x SET a = 1 -- '", null, /Comments/);
  refuses('-- inside a string still counts (refused, by design)', "INSERT INTO staging.t VALUES ('--')", null, /Comments/);
  refuses('an unterminated string', "INSERT INTO staging.t VALUES ('abc", null, /unterminated string/);
  refuses('an unterminated [name]', 'INSERT INTO [staging.t SELECT 1', null, /unterminated/);
  refuses('a string ending early cannot hide a write: doubled quotes are read as SQL Server reads them',
    "INSERT INTO staging.t VALUES ('a'''); DELETE FROM dbo.x WHERE 1 = 1; SELECT ('')");
  refuses('a quoted name that only looks like staging', 'INSERT INTO [staging.t] SELECT 1');
  refuses('"staging"."t" with a space-padded schema', 'INSERT INTO [staging ].t SELECT 1');
  refuses('a Unicode look-alike schema name (Cyrillic а)', 'INSERT INTO stаging.t SELECT 1');
  refuses('an empty statement', '   ');
}

section('5. PostgreSQL');
{
  allows('CREATE SCHEMA IF NOT EXISTS', 'CREATE SCHEMA IF NOT EXISTS staging', 'postgres');
  allows('CREATE TABLE IF NOT EXISTS, and AS SELECT', 'CREATE TABLE IF NOT EXISTS staging.t (a int); CREATE TABLE staging.u AS SELECT a FROM public.x', 'postgres');
  allows('INSERT … ON CONFLICT DO UPDATE', 'INSERT INTO staging.t (a) SELECT a FROM public.x ON CONFLICT (a) DO UPDATE SET a = EXCLUDED.a', 'postgres');
  allows('TRUNCATE without TABLE, and a list', 'TRUNCATE staging.a, staging.b', 'postgres');
  allows('UPDATE … FROM', 'UPDATE staging.t SET b = x.b FROM public.x WHERE x.a = staging.t.a', 'postgres');
  allows('DELETE … USING', 'DELETE FROM staging.t USING public.x WHERE x.a = staging.t.a', 'postgres');
  allows('quoted lower-case schema', 'INSERT INTO "staging"."T" SELECT 1', 'postgres');
  allows('a two-part DROP INDEX (PostgreSQL\'s own form)', 'DROP INDEX staging.ix_t', 'postgres');
  allows('CREATE UNLOGGED TABLE', 'CREATE UNLOGGED TABLE staging.t (a int)', 'postgres');
  refuses('quoted "Staging" is a DIFFERENT schema in PostgreSQL', 'INSERT INTO "Staging".t SELECT 1', 'postgres', /outside/);
  refuses('a data-modifying CTE that deletes from a real table', 'WITH d AS (DELETE FROM public.x RETURNING *) INSERT INTO staging.t SELECT * FROM d', 'postgres', /outside/);
  refuses('a second statement that is not a known kind (VACUUM)', 'INSERT INTO staging.t SELECT 1; VACUUM public.x', 'postgres', /VACUUM/);
  refuses('COPY', 'COPY staging.t FROM \'/etc/passwd\'', 'postgres');
  refuses('CALL', 'INSERT INTO staging.t SELECT 1; CALL public.p()', 'postgres');
  refuses('DO block', 'DO $$ BEGIN DELETE FROM public.x; END $$', 'postgres');
  refuses('a dollar-quoted string', "INSERT INTO staging.t VALUES ($q$ it's $q$)", 'postgres', /dollar/);
  refuses('an E-string backslash escape that would hide a write', "INSERT INTO staging.t VALUES (E'\\''); DELETE FROM public.x; SELECT ('')", 'postgres', /Backslashes/);
  refuses('setval on a real sequence', "INSERT INTO staging.t SELECT setval('public.seq', 1)", 'postgres', /setval/);
  refuses('dblink_exec', "INSERT INTO staging.t SELECT dblink_exec('host=x', 'DELETE FROM y')", 'postgres');
  refuses('DROP … CASCADE', 'DROP SCHEMA staging CASCADE', 'postgres', /CASCADE/);
  refuses('TRUNCATE … CASCADE', 'TRUNCATE staging.t CASCADE', 'postgres', /CASCADE/);
  refuses('ALTER TABLE … SET SCHEMA public', 'ALTER TABLE staging.t SET SCHEMA public', 'postgres', /moved/);
  refuses('a temporary table', 'CREATE TEMP TABLE t AS SELECT 1', 'postgres', /temporary/);
  refuses('SET ROLE', 'SET ROLE admin; INSERT INTO staging.t SELECT 1', 'postgres', /SET ROLE/);
  refuses('GRANT', 'GRANT ALL ON staging.t TO PUBLIC', 'postgres', /must start with/);
  refuses('a #name is not a PostgreSQL temp table', 'INSERT INTO #t SELECT 1', 'postgres');
}

section('6. The vocabulary is what the header says it is');
{
  check('every word on the SQL Server list is reserved there (or named as the Service Broker exception)',
    S.NEVER_MSSQL.every(w => /^[A-Z_]+$/.test(w)) && S.NEVER_MSSQL.indexOf('RECEIVE') !== -1 && S.NEVER_MSSQL.indexOf('SWITCH') === -1);
  check('a write may only begin with the listed words', S.FIRST_WORDS.join() === 'SELECT,WITH,INSERT,UPDATE,DELETE,CREATE,DROP,ALTER,TRUNCATE,IF,BEGIN,DECLARE,SET');
  const tz = S.tokenize("N'a''b' [c]]d] \"e\"\"f\" @v #t x.y", 'sqlserver').tokens;
  check('the tokenizer reads strings, brackets, quotes, variables and temps', tz.map(t => t.t).join() === 'str,qid,qid,var,temp,word,op,word'
    && tz[1].v === 'c]d' && tz[2].v === 'e"f', JSON.stringify(tz));
}

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
