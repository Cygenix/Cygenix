// netlify/edge-functions/analyse.js
// Edge functions run on Deno at Netlify's edge network.
// They have NO timeout limit on the free plan — perfect for Claude API calls.
// Uses ES module syntax (not CommonJS).

import { verifyRequestAuth } from './_lib/verify-entra.js';

/* The model is a pinned snapshot and will be retired eventually; keep it in
   ONE place and overridable without a redeploy. Set ANTHROPIC_MODEL in the
   Netlify environment to change it. */
const MODEL = (typeof Deno !== 'undefined' && Deno.env.get('ANTHROPIC_MODEL'))
  || 'claude-sonnet-5';

export default async function handler(request, context) {
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
  };

  if (request.method === 'OPTIONS') {
    return new Response('', { status: 200, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ error: 'Method not allowed' }), {
      status: 405,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  // ── DISABLED 2026-08-28 ───────────────────────────────────────────────
  //
  // This endpoint used to read ANTHROPIC_API_KEY from the Netlify environment
  // and spend the site owner's personal Anthropic account. The comment that
  // used to sit here said so outright: "this endpoint spends the site's
  // Anthropic API budget". Being signed in was the only thing standing
  // between any user of the site and that bill.
  //
  // It is blocked rather than converted to a user-supplied key because the
  // only thing left in the repository that calls /api/analyse is the
  // Diagnostics page, which calls it to check that it answers. Nothing in the
  // product depends on it. An endpoint whose sole caller is its own health
  // check is not worth a key path.
  //
  // To bring it back: take the key from the x-anthropic-key header (see
  // azure-function/src/user-anthropic-key.js for the shape), never from the
  // environment.
  return new Response(JSON.stringify({
    error: 'This feature is temporarily disabled while user-supplied API key ' +
           'support is being wired up. It will be re-enabled shortly.',
    code: 'USER_KEY_REQUIRED',
  }), { status: 503, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

}

function formatSize(b) {
  if (!b || isNaN(b)) return 'unknown size';
  if (b < 1024) return b + 'B';
  if (b < 1048576) return (b / 1024).toFixed(1) + 'KB';
  return (b / 1048576).toFixed(1) + 'MB';
}

function getExt(name) {
  if (!name) return 'FILE';
  return name.split('.').pop().toUpperCase().slice(0, 4) || 'FILE';
}
