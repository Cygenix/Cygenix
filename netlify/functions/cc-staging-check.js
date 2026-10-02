/* ============================================================================
   cc-staging-check.js — may this load script be saved as a scheduled job?
   ----------------------------------------------------------------------------
   Oct-2026. A Dev Console staging session ends with a load script: every
   staging table rebuilt from the source, in load order. "Save as job" turns
   that script into an ordinary Cygenix SQL job, which Task Agent can run on
   a schedule — with no Claude and no Anthropic key involved on the night.

   That job will run unattended with the connection's own login, which can
   write anywhere. So before the page saves it, the script is held to the
   same rule as the session that wrote it — inside the staging schema,
   anything; everywhere else, nothing — by the same reader
   (lib/staging-sql.js) the bridge uses on every live statement. This
   function is that check and nothing else: it reads no database, stores
   nothing, and answers yes with the list of what the script writes, or no
   with the reason.

   Who may ask: a signed-in member whose role could open a staging session
   (claudecode.use, mutating — the same grant the gate asks for), in an
   organisation that has the Dev Console switched on for that role. The
   check costs nothing, but its "yes" is what the page shows before saving,
   and nobody else has a reason to hold one.
   ========================================================================== */
'use strict';

const authz = require('./lib/authz');
const tenancy = require('./lib/tenancy');
const { checkStagingWrite, stagingSchemaProblem } = require('./lib/staging-sql');

const HEADERS = { 'Content-Type': 'application/json' };
const reply = (statusCode, data) => ({ statusCode, headers: HEADERS, body: JSON.stringify(data) });
const MAX_SQL = 500000;

exports.handler = async function (event) {
  if (event.httpMethod !== 'POST') return reply(405, { error: 'POST only' });

  let ctx;
  try {
    ctx = await authz.authorize(event, { route: 'cc-staging-check', action: 'claudecode.use', mutating: true });
  } catch (e) {
    return authz.errorResponse(e, HEADERS);
  }
  const policy = tenancy.normaliseClaudeCode(ctx.tenant.claudeCode);
  if (!policy.enabled || !ctx.actor.roles.some(r => policy.roles.indexOf(r) !== -1)) {
    return reply(403, { error: 'Not enabled for your role — ask an Owner to enable it in Governance.' });
  }

  let body;
  try { body = JSON.parse(event.body || '{}'); }
  catch (e) { return reply(400, { error: 'Body is not JSON' }); }

  const sql = String(body.sql == null ? '' : body.sql);
  const schema = String(body.schema == null ? '' : body.schema);
  const dialect = body.dbType === 'postgres' ? 'postgres' : 'sqlserver';
  if (!sql.trim()) return reply(400, { error: 'There is no script to check.' });
  if (sql.length > MAX_SQL) return reply(413, { error: 'The script is over ' + MAX_SQL + ' characters.' });
  const why = stagingSchemaProblem(schema, dialect);
  if (why) return reply(400, { error: why });

  try {
    const r = checkStagingWrite(sql, { schema, dialect });
    if (!r.ok) return reply(200, { ok: false, why: r.why });
    if (!r.writes.length) return reply(200, { ok: false, why: 'The script changes nothing, so there is nothing to schedule.' });
    return reply(200, { ok: true, writes: r.writes });
  } catch (e) {
    return reply(500, { error: e.message, stack: e.stack });
  }
};
