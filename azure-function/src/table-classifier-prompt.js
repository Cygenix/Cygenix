// table-classifier-prompt.js
//
// THE WORDS AND SETTINGS FOR "WHICH MODULE DOES THIS TABLE BELONG TO?"
//
// This file is the one place to refine how Conversion Templates' Suggest all
// (and the per-module Suggest button, which is the same thing with one
// module) asks Claude to sort a target database's tables into business
// subject areas. Change the wording here; nothing in the page or the
// plumbing (table-classifier.js) needs to change with it.
//
// WHAT YOU CAN SAFELY CHANGE
//   CLASSIFY_SYSTEM   the instructions. Keep the JSON field names as they are
//                     (table, modules, confidence, required, reason) — the
//                     answer is checked against OUTPUT_SCHEMA in
//                     table-classifier.js, and a renamed field is dropped.
//   MODEL / EFFORT    which Claude model, and how hard it thinks. Accuracy
//                     matters more than cost here (it runs about once per
//                     project, through the Batches API at half price).
//   TABLES_PER_CHUNK  how many tables go into one request. Fewer means each
//                     request has less to juggle; more means fewer requests.
//
// WHAT MUST STAY TRUE, WHATEVER THE WORDING
//   - Target-agnostic. No product's table names, prefixes, or module→table
//     lists, here or anywhere else. Claude works from the live schema, the
//     module names and the module notes, and nothing else.
//   - Claude may only name tables it was given and modules it was given. The
//     code drops anything else, so a prompt that invites invention just
//     produces fewer suggestions.

'use strict';

// The current Opus. Overridable per deployment without a code change.
const MODEL = process.env.ANTHROPIC_MODEL_CLASSIFY || 'claude-opus-5-5';

// 'high' rather than the model's default: this is a judgement over many
// tables at once, and accuracy is the point.
const EFFORT = 'high';

// Room for the thinking as well as the answer. A Batches request has no HTTP
// timeout to fit inside, so this can be generous.
const MAX_TOKENS = 32000;

// Tables per request. Every request also carries the full list of modules,
// so each table is judged against every subject area at once.
const TABLES_PER_CHUNK = 40;

const CLASSIFY_SYSTEM = `You help a data-migration analyst sort the tables of a TARGET database into the business modules (subject areas) of a migration.

You are given:
- the modules in play: each has a name and sometimes notes written by the analyst;
- a batch of tables from the target database: each with its columns (name:type), primary key, row count where known, the tables it references through foreign keys, and the tables that reference it.

You see only part of the database at a time, but the full list of modules every time. Judge every table in the batch against all the modules together and put it where it fits best.

For EVERY table in the batch, return one entry:
- "table": the table name exactly as given.
- "modules": the name(s) of the module(s) the table belongs to, exactly as given. Usually one. More than one only when the table genuinely holds data for each of them (for example a shared lookup or a cross-module link table). An empty list when it belongs to none of these modules.
- "confidence": "high" (clearly this module's data), "medium" (probably, or shared), or "low" (plausible but doubtful).
- "required": true if loading this module would not be complete without this table; false if it is optional, historical, audit/log, staging, archive or derived data.
- "reason": one short line (at most 90 characters) naming the evidence: columns, keys or relationships.

How to judge:
- Use the table's columns and relationships, not just its name. Names can be abbreviated or cryptic.
- Foreign keys are strong evidence. A child or detail table usually belongs with its parent's module; a lookup table referenced mainly by one module's tables belongs with that module.
- The module notes are the analyst's own description of what is in scope. Follow them.
- System, security, configuration, logging, audit and workflow-engine tables usually belong to no module unless a module's notes say otherwise.
- Never invent a table or a module. Never rename one.`;

module.exports = { MODEL, EFFORT, MAX_TOKENS, TABLES_PER_CHUNK, CLASSIFY_SYSTEM };
