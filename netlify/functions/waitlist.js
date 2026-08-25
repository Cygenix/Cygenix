// netlify/functions/waitlist.js
//
// The one genuinely anonymous call in the product: a visitor with no account
// submits the register page's waitlist form.
//
// It cannot go through data-proxy, which demands a verified Entra token by
// design. But it also must not be the reason a Function App host key sits in
// register.html — which is exactly what it was. So it gets its own door: the
// key stays in the environment, and the browser posts here.
//
// Anonymous does not mean unbounded. This function forwards one shape of
// payload to one action, and nothing else.

'use strict';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};
const fail = (msg, code) => ({
  statusCode: code || 500, headers: CORS, body: JSON.stringify({ error: msg }),
});

const API_BASE = process.env.CYGENIX_DATA_API_BASE
  || 'https://cygenix-db-api-e4fng7a4edhydzc4.uksouth-01.azurewebsites.net/api/data';
const FN_KEY = process.env.CYGENIX_DATA_FN_KEY || '';

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };
  if (event.httpMethod !== 'POST') return fail('Method not allowed', 405);
  if (!FN_KEY) return fail('CYGENIX_DATA_FN_KEY is not configured for this deployment.', 503);

  // Parse and re-serialise rather than passing the body through. An open
  // endpoint that forwards whatever it is given is a relay for whatever the
  // caller wants; this one forwards a waitlist signup.
  let body;
  try { body = JSON.parse(event.body || '{}'); }
  catch (e) { return fail('Malformed request', 400); }

  const email = String(body.email || '').trim().toLowerCase();
  if (!email || email.length > 254 || !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) {
    return fail('A valid email address is required', 400);
  }
  const payload = {
    email: email,
    name:    String(body.name    || '').slice(0, 120),
    company: String(body.company || '').slice(0, 120),
    source:  String(body.source  || '').slice(0, 60),
    note:    String(body.note    || '').slice(0, 500),
  };

  try {
    const res = await fetch(API_BASE + '/waitlist?code=' + encodeURIComponent(FN_KEY), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const text = await res.text();
    return { statusCode: res.status, headers: CORS, body: text || '{}' };
  } catch (e) {
    return fail('Upstream error: ' + e.message, 502);
  }
};
