/**
 * auth-config.js
 * Azure Entra External ID configuration for Cygenix.
 * Single source of truth — all auth files reference this.
 */
const CYGENIX_AUTH = {
  clientId:   '43154cd5-4111-45cf-8c9e-0a243c6a4d64',
  tenantId:   'b809b12b-5ff1-46c0-9315-c40d007b5bb8',
  tenantName: 'cygenix',
  userFlow:   'cygenix_signin',
  get authority() {
    return `https://${this.tenantName}.ciamlogin.com/${this.tenantId}`;
  },
  redirectUri:  'https://cygenix.co.uk/login.html',
  postLogoutRedirectUri: 'https://cygenix.co.uk/login.html',
  scopes: ['openid', 'profile', 'email'],
};
