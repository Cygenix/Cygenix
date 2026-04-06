// netlify/functions/health.js
exports.handler = async function () {
  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      status: 'ok',
      apiKeyConfigured: !!process.env.ANTHROPIC_API_KEY,
      timestamp: new Date().toISOString(),
    }),
  };
};
