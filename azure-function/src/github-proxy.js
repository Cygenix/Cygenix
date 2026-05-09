// ─────────────────────────────────────────────────────────────────────────────
// github-proxy.js — Phase 1a (DIAGNOSTIC v2)
// ─────────────────────────────────────────────────────────────────────────────
// Stripped-down version. The goal is to prove that this file can load and
// register a route on Azure Flex Consumption. Original v1 failed silently
// during module load, so all features are removed. Once /api/github/ping
// returns 200, we add the real handlers back in one step at a time.
//
// What's intentionally NOT here vs v1:
//   - Buffer.from(...) calls          (suspect #1 — base64 encoding)
//   - require('@azure/cosmos')        (suspect #2 — module load)
//   - getCosmosContainer / getUserId  (suspect #3 — Cosmos init)
//   - All gh()/action handlers        (suspect #4 — fetch / native APIs)
//
// What IS here:
//   - The same `const { app } = require('@azure/functions');` import as
//     every other module file in this project
//   - The exact same CORS / ok / err helpers
//   - One route: POST/OPTIONS /api/github/ping
// ─────────────────────────────────────────────────────────────────────────────
 
const { app } = require('@azure/functions');
 
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-user-id',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type':                 'application/json'
};
 
const ok  = (body)            => ({ status: 200, headers: CORS, body: JSON.stringify(body) });
const err = (code, msg, extra) => ({
  status: code,
  headers: CORS,
  body: JSON.stringify({ error: msg, ...(extra || {}) })
});
 
// Single minimal route. If THIS doesn't register, the problem is something
// fundamental about how this file is being loaded — not anything inside it.
app.http('github', {
  methods:   ['POST', 'OPTIONS', 'GET'],
  authLevel: 'function',
  route:     'github/{action}',
  handler: async (req, ctx) => {
    if (req.method === 'OPTIONS') return { status: 200, headers: CORS, body: '' };
 
    const action = req.params.action;
    ctx.log(`github-proxy: received action = ${action}`);
 
    if (action === 'ping') {
      return ok({
        ok:        true,
        message:   'github-proxy is alive',
        node:      process.version,
        timestamp: new Date().toISOString()
      });
    }
 
    return err(404, `Unknown action: ${action}`);
  }
});
