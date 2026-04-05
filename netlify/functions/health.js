// netlify/functions/health.js
exports.handler = async function () {
  const hasKey = !!process.env.ANTHROPIC_API_KEY;
  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      status: 'ok',
      apiKeyConfigured: hasKey,
      timestamp: new Date().toISOString(),
    }),
  };
};
