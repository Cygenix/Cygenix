/* ============================================================
   Sidebar entry to add for Agentive Migration
   ============================================================
   Drop this into cygenix-sidebar.js alongside the existing nav
   items. The exact insertion point depends on your current file
   structure — paste this object into the array of nav items.
   
   Suggested position: just below "Object Mapping" and above
   "Project Artifacts" (Inventory). It belongs with the other
   "do something with your data" tools rather than at the end.
*/

// 1) Add to your nav items array:
{
  id: 'agentive_migration',
  label: 'Agentive Migration',
  href: '/agentive_migration.html',
  view: 'view-agentive-migration',
  badge: 'Beta',
  requiresAiEnabled: true,   // hide if tenant or user has AI off
  // Inline SVG so we don't add a new icon dependency.
  icon: `<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"
              fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"
              stroke-linejoin="round" aria-hidden="true">
           <path d="M11 12h2a2 2 0 1 0 0-4h-3c-.6 0-1.1.2-1.4.6L3 14"/>
           <path d="m7 18 1.6-1.4c.3-.4.8-.6 1.4-.6h4c1.1 0 2.1-.4 2.8-1.2l4.6-4.4a2 2 0 0 0-2.75-2.91l-4.2 3.9"/>
           <path d="m2 13 6 6"/>
         </svg>`
},

/* ============================================================
   2) Feature-gating filter
   ============================================================
   In the function that renders the sidebar, filter out items
   marked requiresAiEnabled when the AI toggles are off.
   
   Reads cached flags from localStorage; the dashboard should
   refresh these on login from /api/me. Defaults to "show" when
   unknown so you can develop the page without the backend yet.
*/

function isAgentiveEnabled() {
  try {
    const flags = JSON.parse(localStorage.getItem('cygenix_feature_flags') || '{}');
    if (flags.tenantAgentiveMigrationEnabled === false) return false;
    if (flags.aiAgentiveEnabled === false) return false;
    return true; // default-on for dev; the backend will lock this down
  } catch (e) {
    return true;
  }
}

// In your sidebar render loop, before mapping items to DOM:
const visibleItems = navItems.filter(item => {
  if (item.requiresAiEnabled && !isAgentiveEnabled()) return false;
  return true;
});

/* ============================================================
   3) Refreshing flags on login
   ============================================================
   In dashboard.html (or wherever you handle post-login bootstrap),
   after MSAL gives you a token, call /api/me and cache the flags:
*/

async function refreshFeatureFlags() {
  try {
    const token = await window.cygenixGetAccessToken();
    const res = await fetch('/api/me', {
      headers: { 'Authorization': `Bearer ${token}` }
    });
    if (!res.ok) return;
    const me = await res.json();
    localStorage.setItem('cygenix_feature_flags', JSON.stringify({
      tenantAgentiveMigrationEnabled: !!(me.tenant && me.tenant.agentiveMigrationEnabled),
      aiAgentiveEnabled: !!(me.user && me.user.aiAgentiveEnabled),
    }));
  } catch (e) {
    console.warn('[Cygenix] could not refresh feature flags:', e.message);
  }
}
