/**
 * auth-config.js
 * Azure Entra External ID configuration for Cygenix.
 * Single source of truth — all auth files reference this.
 */
const CYGENIX_AUTH = {
  clientId:   'f3478996-b2b5-4b21-9a23-a6b97a0e5b13',
  tenantId:   'fc8dfc7a-645f-4a5c-8f59-6762f97c803f',
  tenantName: 'cygenix',
  userFlow:   'cygenix_signin',
  get authority() {
    return `https://${this.tenantName}.ciamlogin.com/${this.tenantId}`;
  },
  redirectUri:  'https://cygenix.co.uk/login.html',
  postLogoutRedirectUri: 'https://cygenix.co.uk/login.html',
  scopes: ['openid', 'profile', 'email'],
};
