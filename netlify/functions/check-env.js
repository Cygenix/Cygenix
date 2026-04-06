// netlify/functions/check-env.js - enhanced diagnostic

exports.handler = async function () {
  const siteID = process.env.SITE_ID || process.env.NETLIFY_SITE_ID;
  const token  = process.env.NETLIFY_FUNCTIONS_TOKEN || process.env.NETLIFY_API_TOKEN;

  // Try to actually use @netlify/blobs
  let blobTest = {};
  try {
    const { getStore } = require('@netlify/blobs');
    blobTest.imported = true;

    const store = getStore({ name: 'diag-test', siteID, token });
    blobTest.storeCreated = true;

    // Try a real write
    await store.set('ping', 'pong');
    blobTest.writeOk = true;

    // Try a real read
    const val = await store.get('ping');
    blobTest.readOk = val === 'pong';

    // Clean up
    await store.delete('ping');
    blobTest.deleteOk = true;

  } catch (e) {
    blobTest.error = e.message;
    blobTest.stack = e.stack?.split('\n').slice(0,5).join(' | ');
  }

  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      siteID_set: !!siteID,
      token_set:  !!token,
      siteID_preview: siteID ? siteID.slice(0,8) + '...' : null,
      token_preview:  token  ? token.slice(0,6)  + '...' : null,
      blobTest,
    }, null, 2)
  };
};