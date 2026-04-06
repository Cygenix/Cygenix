// netlify/functions/check-env.js
// Temporary diagnostic — DELETE after fixing

exports.handler = async function () {
  const siteId   = process.env.NETLIFY_SITE_ID;
  const token    = process.env.NETLIFY_API_TOKEN;
  const siteIdAlt = process.env.SITE_ID;

  // Show all NETLIFY_ env vars without revealing full token value
  const allEnv = Object.keys(process.env)
    .filter(k => k.startsWith('NETLIFY') || k === 'SITE_ID')
    .reduce((acc, k) => {
      const val = process.env[k];
      acc[k] = val ? val.slice(0, 6) + '...' + val.slice(-4) : '(empty)';
      return acc;
    }, {});

  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      NETLIFY_SITE_ID_set:   !!siteId,
      NETLIFY_API_TOKEN_set: !!token,
      SITE_ID_set:           !!siteIdAlt,
      allNetlifyVars:        allEnv,
      nodeVersion:           process.version,
    }, null, 2)
  };
};
