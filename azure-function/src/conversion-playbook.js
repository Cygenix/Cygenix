/* ============================================================================
   conversion-playbook.js — what a Dev Console STAGING session is for, in
   the words Claude is given at the start of one
   ----------------------------------------------------------------------------
   Oct-2026. This is the "conversion.md" of a staging session: the standing
   brief that turns "here is a template and a database" into the same piece
   of work every time — read the template, find the data, build the staging
   tables, load them, check them, report. It is a .js module rather than a
   .md file because the Function App's deploy zip leaves out every *.md
   (.github/workflows/main_cygenix-db-api.yml), and a brief that silently
   failed to ship would be worse than none.

   It says WHAT and in what order, and where the person must be asked. It
   does not say how to map any particular system's data: that is the part
   Claude works out from the source schema, and writing it down here would
   only make it worse at it. It is target-agnostic — nothing below names a
   product, a module or a table.

   The rules that matter for safety are not in here. They are enforced by
   the bridge (netlify/functions/cc-mcp.js and lib/staging-sql.js) whatever
   this text says; the brief only explains them so that Claude works with
   them rather than discovering them one refusal at a time.
   ========================================================================== */
'use strict';

function conversionPlaybook(schema, dbType) {
  const s = String(schema);
  const sql = dbType === 'postgres' ? 'PostgreSQL' : 'SQL Server';
  return [
    'THIS IS A STAGING SESSION. The job: build the staging tables described by this project\'s Conversion Template inside the '
      + 'schema "' + s + '" of this ' + sql + ' database, and populate them with the corresponding data found elsewhere in the same '
      + 'database. A staging table has the target system\'s table shape — the target\'s column names and types, every column '
      + 'nullable — so that standard import scripts can load the target from it. The data comes from this database\'s own tables.',

    'WHAT YOU MAY CHANGE. Anything inside "' + s + '": create, load, empty, drop and rebuild its tables. Nothing else — every other '
      + 'schema is read-only, and Cygenix refuses any statement that writes outside "' + s + '", with the reason. Write every staging '
      + 'name in full as ' + s + '.<table>. A statement that changes "' + s + '" runs in a transaction, must not contain comments, '
      + 'and is stopped and undone after about 19 seconds — load big tables in slices (by key range or date). EXEC, MERGE and '
      + 'UPDATE/DELETE through an alias are refused; write UPDATE ' + s + '.<table> SET … FROM ' + s + '.<table> JOIN … instead.',

    'HOW TO GO ABOUT IT:\n'
      + '1. Read the template with get_conversion_template (no arguments first). Tell the user, briefly: which template, how many '
      + 'modules and tables, and the load order. If the template has no column detail for a table, say so — it cannot be built yet.\n'
      + '2. Survey this database: list_tables, row counts, describe the tables that look relevant. Work out where each staging '
      + 'table\'s data lives — which tables, which joins, which lookups and code translations. Use the notes in the template.\n'
      + '3. Create "' + s + '" if it does not exist (CREATE SCHEMA ' + s + ' on its own), then the staging tables, using the '
      + 'create_table statement the template tool gives for each one.\n'
      + '4. Before loading, show the user the mapping for the first module (or the first few tables): for each staging column, '
      + 'the source expression, or "no source found". Say how confident you are and what you assumed. Wait for them to agree or '
      + 'correct you. Once they are happy with the approach, carry on with the rest without asking table by table, but stop and '
      + 'ask whenever something is genuinely ambiguous.\n'
      + '5. Load each table in the template\'s load order with INSERT INTO ' + s + '.<table> (…) SELECT … FROM … — the data moves '
      + 'inside the database, never through you. To reload, TRUNCATE TABLE ' + s + '.<table> first. Convert types explicitly where '
      + 'the source and target differ, and prefer TRY_CONVERT/TRY_CAST over a load that fails on one bad row.\n'
      + '6. Check every table you load: rows loaded against rows expected from the source; required-in-target columns that came '
      + 'out NULL; duplicates on the target\'s key columns; values truncated or that would not convert. Fix what is a mapping '
      + 'mistake; report what is a data problem.\n'
      + '7. Finish with a report, also saved as /mnt/session/outputs/staging-report.md, and the mapping as '
      + '/mnt/session/outputs/staging-mapping.csv (staging table, staging column, source expression, notes): per table, where '
      + 'the data came from, rows loaded, columns left empty and why, and the problems found. Write the same results as '
      + '/mnt/session/outputs/conversion-report.json, which Cygenix saves to its Conversion Reports — JSON with exactly these '
      + 'fields: {"template": {"name": "", "version": 0}, "summary": "", "tables": [{"staging_table": "", "target_table": "", '
      + '"source_tables": [""], "rows_loaded": 0, "rows_expected": 0, "status": "loaded | partial | failed | not_loaded", '
      + '"notes": "", "columns": [{"column": "", "source": "", "transform": "", "notes": ""}]}], "warnings": [""]}.',

    'GROUND RULES. Never invent data: a column with no source stays NULL and is listed in the report. Leave identity columns '
      + 'empty unless the template\'s notes say otherwise. Keep the target\'s column names exactly as the template gives them. '
      + 'Keep the user informed after each module in a line or two — what was loaded, how many rows, anything that needs them.',
  ].join('\n\n');
}

module.exports = { conversionPlaybook };
