#!/usr/bin/env node
/* ============================================================================
   storage-inventory.js — generates docs/storage-inventory.md by reading the
   source, not by anyone remembering to update a list.
   ----------------------------------------------------------------------------
   WI-1's first acceptance criterion is an inventory of every client-side
   storage key with its class. Hand-written, that document is accurate on the
   day it is written and misleading a month later — which is precisely the
   failure mode WI-9 exists to fix elsewhere in the product. So it is
   generated, and tests/storage-inventory.test.js fails the build when the
   committed copy is stale or when a key appears that nobody has classified.

   The three classes are the brief's:

     A — Authoritative   losing it loses customer work. Must have a server
                         source of truth.
     B — Cache           derived from server state; safe to lose.
     C — Preference      per-viewer UI convenience. localStorage is correct.

   Plus one the brief does not name but the codebase needs:

     S — Secret          credential material. Must not be in browser storage
                         at all. Every entry here is a finding, not a design.

   Run: node scripts/storage-inventory.js
   ========================================================================== */
'use strict';

const fs = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..');
const PUB = path.join(ROOT, 'public');

/* ── The classification table ───────────────────────────────────────────────
   Every key the scanner finds must appear here or the test fails. A prefix
   entry ends with '*' and covers keys built at runtime. */
const CLASSES = {
  // ── S — credential material. Findings, all of them. ──────────────────────
  'cygenix_conn_string':     ['S', 'Target connection string, in plain text', 'WI-10: move to a Key Vault reference'],
  'cygenix_src_conn_string': ['S', 'Source connection string, in plain text', 'WI-10: move to a Key Vault reference'],
  'cygenix_fn_key':          ['S', 'Azure Function key for a user-supplied endpoint', 'WI-10'],
  'cygenix_api_key':         ['S', 'User-supplied API key', 'WI-10'],
  'anthropic_api_key':       ['S', 'Anthropic API key (legacy key name)', 'WI-10: server-side proxy'],
  'cygenix_anthropic_key':   ['S', 'Anthropic API key', 'WI-10: server-side proxy'],
  'cygenix_pending_token':   ['S', 'Auth token in transit between pages', 'short-lived; clear on consume'],

  // ── A — authoritative. Losing it loses customer work. ────────────────────
  'cygenix_jobs':                ['A', 'Migration jobs', 'synced (SYNC_KEYS)'],
  'cygenix_projects':            ['A', 'The user’s project list', 'synced (SYNC_KEYS)'],
  'cygenix_conv_project':        ['A', 'The currently-open project blob', 'synced (SYNC_KEYS)'],
  'cygenix_project_settings':    ['A', 'Per-project settings', 'synced (SYNC_KEYS)'],
  'cygenix_project_plan':        ['A', 'Project plan grid', 'synced (SYNC_KEYS)'],
  'cygenix_project_connections': ['A', 'Per-project connection metadata', 'synced (SYNC_KEYS)'],
  'cygenix_saved_connections':   ['A', 'Saved connection profiles', 'synced (SYNC_KEYS)'],
  'cygenix_validation_sources':  ['A', 'Validation sources', 'synced (SYNC_KEYS)'],
  'cygenix_wasis_rules':         ['A', 'Was/Is substitution rules', 'synced (SYNC_KEYS)'],
  'cygenix_sql_scripts':         ['A', 'Saved SQL scripts', 'synced (SYNC_KEYS)'],
  'cygenix_issues':              ['A', 'Tracked issues', 'synced (SYNC_KEYS)'],
  'cygenix_inventory':           ['A', 'Project artifact inventory', 'synced (SYNC_KEYS)'],
  'cygenix_sys_params':          ['A', 'System parameters', 'synced (SYNC_KEYS)'],
  'cygenix_performance':         ['A', 'Performance settings', 'synced (SYNC_KEYS)'],
  'cygenix_last_snapshots':      ['A', 'Recent project snapshots', 'synced (SYNC_KEYS)'],
  'cygenix_objmap_wip_*':        ['A', 'Object-mapping work in progress', 'NOT SYNCED — WI-1'],
  'cygenix_batches_v1':          ['A', 'Saved batch arrangements', 'NOT SYNCED — WI-1'],
  'cygenix_effort_estimates_v1': ['A', 'Saved Configurator estimates', 'NOT SYNCED — WI-1'],
  'cygenix_profiles_v1':         ['A', 'Connection profiles', 'NOT SYNCED — WI-1'],
  'cygenix_report':              ['A', 'Generated conversion report', 'NOT SYNCED — WI-1'],
  'cygenix_report_config':       ['A', 'Report configuration', 'NOT SYNCED — WI-1'],
  'cygenix_backup_history':      ['A', 'Restore-module backup history', 'NOT SYNCED — WI-1'],
  'cygenix_datastream_v1::*':    ['A', 'Data Stream configuration and state', 'demo-only in v1'],
  'cygenix_datastream_wip_*':    ['A', 'Stream Designer draft', 'draft only; discardable'],

  // ── B — cache. Derived; safe to lose. ────────────────────────────────────
  'cygenix_schema_*':            ['B', 'Cached database schema', 're-read from the database'],
  'cygenix_insight_*':           ['B', 'Cached insight results', 'recomputed'],
  'cygenix_ps_ai_*':             ['B', 'Cached AI project-summary text', 'regenerated'],
  'cygenix_cleansing_handoff':   ['B', 'Hand-off payload between screens', 'transient'],
  'cygenix_pending_sql':         ['B', 'SQL queued for the editor', 'transient'],
  'cygenix_sql_editor_draft':    ['B', 'Editor draft text', 'transient'],
  'cygenix_projects_migrated':   ['B', 'One-time migration marker', 'idempotent'],
  'cygenix_wiped_snapshot':      ['B', 'Recovery snapshot after a wipe', 'recovery aid'],
  'cygenix_job_scope':           ['B', 'Current job scope selection', 'recomputed'],

  // ── C — preference. localStorage is the right home. ──────────────────────
  'cygenix_app_prefs':                 ['C', 'Theme and app preferences', ''],
  'cygenix_sidebar_collapsed':         ['C', 'Sidebar collapsed state', ''],
  'cygenix_sidebar_pinned_v1':         ['C', 'Pinned sidebar favourites', ''],
  'cygenix_sidebar_pinned_hint_v1':    ['C', 'First-run hint dismissal', ''],
  'cygenix_sidebar_closed_groups':     ['C', 'Collapsed nav groups', ''],
  'cygenix_jobs_panel_collapsed':      ['C', 'Jobs panel collapsed', ''],
  'cygenix_sql_panel_collapsed':       ['C', 'SQL panel collapsed', ''],
  'cygenix_job_sort':                  ['C', 'Job list sort order', ''],
  'cygenix_schemaexp_atlas_smfilters': ['C', 'Schema Explorer filter state', ''],
  'cygenix_onboarded':                 ['C', 'Onboarding completed', ''],
  'cygenix_cookie_consent':            ['C', 'Cookie consent record', ''],
  'cygenix_active_project_id':         ['C', 'Which project is open', 'a pointer, not the data'],
  'cygenix_current_project_id':        ['C', 'Legacy project pointer', ''],
  'cygenix_active_project':            ['C', 'Legacy project pointer', ''],
  'cygenix_project_name':              ['C', 'Display name of the open project', ''],
  'cygenix_client_name':               ['C', 'Client name for reports', ''],
  'cygenix_analyst_name':              ['C', 'Analyst name for reports', ''],
  'cygenix_conn_mode':                 ['C', 'Connection mode (direct/function)', ''],
  'cygenix_src_conn_mode':             ['C', 'Source connection mode', ''],
  'cygenix_fn_url':                    ['C', 'Function URL (not the key)', ''],
  'cygenix_src_fn_url':                ['C', 'Source function URL', ''],
  'cygenix_redirect_after_login':      ['C', 'Post-login destination', ''],
  'cygenix_redirect_after_plan':       ['C', 'Post-checkout destination', ''],
  'cygenix_just_signed_out':           ['C', 'Sign-out flag', ''],
  'cygenix_whoami_ok':                 ['C', 'Tier-gate pass cache (sessionStorage)', ''],
  'cyg_goto':                          ['C', 'Cross-page navigation intent', ''],

  // ── Session/identity. Held by MSAL and the auth layer. ───────────────────
  'cygenix_token':          ['C', 'Session marker', 'MSAL holds the real token'],
  'cygenix_expires':        ['C', 'Session expiry marker', ''],
  'cygenix_user':           ['C', 'Signed-in user (email, name)', ''],
  'cygenix_user_email':     ['C', 'Signed-in email', ''],
  'cygenix_active_user':    ['C', 'Signed-in user tag', ''],
  'cygenix_entra_account':  ['C', 'MSAL account record', ''],
  'cygenix_agentive_migration': ['B', 'Agentive migration run state', 'server-side run is the truth'],
};

const CLASS_NAME = {
  S: 'Secret — must not be in browser storage',
  A: 'Authoritative — losing it loses customer work',
  B: 'Cache — derived, safe to lose',
  C: 'Preference — per-viewer convenience',
};

/* ── Scan ───────────────────────────────────────────────────────────────── */
function scan() {
  const found = new Map();   // key → Set(file)
  const files = fs.readdirSync(PUB).filter(f => /\.(js|html)$/.test(f));
  const re = /(?:localStorage|sessionStorage)\s*\.\s*(?:getItem|setItem|removeItem)\s*\(\s*['"`]([A-Za-z0-9_.:@-]+)/g;
  for (const f of files) {
    const src = fs.readFileSync(path.join(PUB, f), 'utf8');
    let m;
    while ((m = re.exec(src))) {
      const k = m[1];
      if (!found.has(k)) found.set(k, new Set());
      found.get(k).add(f);
    }
  }
  return found;
}

/* A key matches a table entry directly, or by a '*' prefix entry. */
function classify(key) {
  if (CLASSES[key]) return { match: key, entry: CLASSES[key] };
  for (const pat of Object.keys(CLASSES)) {
    if (pat.endsWith('*') && key.startsWith(pat.slice(0, -1))) {
      return { match: pat, entry: CLASSES[pat] };
    }
  }
  return null;
}

function build() {
  const found = scan();
  const rows = [];
  const unclassified = [];
  for (const [key, files] of [...found.entries()].sort()) {
    const c = classify(key);
    if (!c) { unclassified.push({ key, files: [...files] }); continue; }
    rows.push({ key, cls: c.entry[0], what: c.entry[1], note: c.entry[2], files: [...files].sort() });
  }
  return { rows, unclassified, found };
}

function render() {
  const { rows, unclassified } = build();
  const byClass = (c) => rows.filter(r => r.cls === c);
  const esc = (s) => String(s).replace(/\|/g, '\\|');

  let out = `# Client-side storage inventory

**GENERATED** by \`node scripts/storage-inventory.js\`. Do not edit by hand — the next run
overwrites it, and \`tests/storage-inventory.test.js\` fails when the committed copy is stale.

Classes are the Product Engineering Brief's, plus **S** for credential material, which the brief does
not name but the codebase contains.

${Object.entries(CLASS_NAME).map(([k, v]) => `- **${k}** — ${v}`).join('\n')}

A key must appear in the table in \`scripts/storage-inventory.js\` or the test fails. That is the
mechanism that stops an unclassified key being added quietly.

`;

  for (const c of ['S', 'A', 'B', 'C']) {
    const list = byClass(c);
    out += `\n## ${c} — ${CLASS_NAME[c]} (${list.length})\n\n`;
    if (c === 'S' && list.length) {
      out += `> Every row here is a **finding**, not a design decision. Browser storage survives the\n`
           + `> tab, the session and the machine's next user. Tracked as WI-10.\n\n`;
    }
    if (c === 'A') {
      out += `> These need a server source of truth. \`cygenix-cosmos-sync.js\` mirrors the keys marked\n`
           + `> *synced* to Cosmos on a 3-second write-behind; the rest are local-only and are WI-1's\n`
           + `> actual scope.\n\n`;
    }
    if (!list.length) { out += '_None._\n'; continue; }
    out += '| Key | What it holds | Server source of truth | Written by |\n|---|---|---|---|\n';
    for (const r of list) {
      const files = r.files.length > 3
        ? r.files.slice(0, 3).join(', ') + ` +${r.files.length - 3}`
        : r.files.join(', ');
      out += `| \`${esc(r.key)}\` | ${esc(r.what)} | ${esc(r.note || '—')} | ${esc(files)} |\n`;
    }
  }

  if (unclassified.length) {
    out += `\n## Unclassified (${unclassified.length})\n\n`
        + `These are in the code but not in the table. The test fails until each is classified.\n\n`;
    for (const u of unclassified) out += `- \`${u.key}\` — ${u.files.join(', ')}\n`;
  }

  out += `\n---\n\n_${rows.length} classified key(s) across \`public/\`._\n`;
  return out;
}

if (require.main === module) {
  const outPath = path.join(ROOT, 'docs', 'storage-inventory.md');
  fs.writeFileSync(outPath, render());
  const { rows, unclassified } = build();
  console.log('storage-inventory: ' + rows.length + ' key(s) → docs/storage-inventory.md'
    + (unclassified.length ? '  (' + unclassified.length + ' UNCLASSIFIED)' : ''));
  if (unclassified.length) process.exitCode = 1;
}

module.exports = { scan, classify, build, render, CLASSES };
