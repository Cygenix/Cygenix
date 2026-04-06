exports.handler = async function () {
  const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
  const token  = process.env.NETLIFY_API_TOKEN; // personal token only — nfp_yp...

  let blobTest = {};
  try {
    const { getStore } = require('@netlify/blobs');
    blobTest.imported = true;
    blobTest.usingToken = token ? token.slice(0,8) + '...' : 'none';

    const store = getStore({ name: 'diag-test', siteID, token });
    blobTest.storeCreated = true;

    await store.set('ping', 'pong');
    blobTest.writeOk = true;

    const val = await store.get('ping');
    blobTest.readOk = val === 'pong';

    await store.delete('ping');
    blobTest.deleteOk = true;

  } catch (e) {
    blobTest.error = e.message;
  }

  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ siteID_preview: siteID?.slice(0,8), blobTest }, null, 2)
  };
};
