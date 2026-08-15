// notify-log.js — the notification audit log.
//
// Every attempt to notify a user — the Resend email from notify.js, and each
// connector delivery from connectors.js — writes one row here, whether it
// succeeded or not. Settings → Notifications reads them back, so a user can
// answer "did my scheduled run try to email me, and what happened?" without
// opening the Cosmos data explorer.
//
// This lives in its own file rather than inside notify.js because notify.js
// registers an HTTP route at import time. Anything that only wants to write a
// log row would otherwise have to load the Functions host to do it.
//
// Never throws. A failed audit write must not break the thing being audited.

let _cosmos = null;
function notificationsContainer() {
  if (!_cosmos) {
    const { CosmosClient } = require('@azure/cosmos');
    _cosmos = new CosmosClient({
      endpoint: process.env.COSMOS_ENDPOINT,
      key:      process.env.COSMOS_KEY,
    });
  }
  return _cosmos
    .database(process.env.COSMOS_DATABASE || 'cygenix')
    .container('notifications');
}

// `channel` says which delivery path this row is about:
//   'resend'  — the built-in email notify.js sends to the schedule owner
//   'webhook' — the Webhook connector
//   'smtp'    — the Email (SMTP) connector
// Rows written before the field existed have none; readers treat that as
// 'resend', which is what they all were.
async function logNotification(row, ctx) {
  try {
    const doc = {
      id: `${Date.now()}_${Math.random().toString(36).slice(2, 10)}`,
      userId:  row.userId,
      type:    row.type,
      channel: row.channel || 'resend',
      to:      row.to      || null,
      subject: row.subject || null,
      status:  row.status,            // 'sent' | 'failed' | 'skipped'
      resendId: row.resendId || null,
      error:    row.error    || null,
      // 'scheduled-run' | 'manual' | undefined — lets the UI say where a
      // message came from, which is the difference the user cares about.
      source:  row.source || null,
      runId:   row.runId  || null,
      sentAt:  new Date().toISOString(),
    };
    await notificationsContainer().items.create(doc);
    return doc;
  } catch (e) {
    ctx && ctx.log && ctx.log('[notify] audit write failed:', e.message);
    return null;
  }
}

module.exports = { logNotification };
