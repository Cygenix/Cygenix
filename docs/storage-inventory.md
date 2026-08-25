# Client-side storage inventory

**GENERATED** by `node scripts/storage-inventory.js`. Do not edit by hand — the next run
overwrites it, and `tests/storage-inventory.test.js` fails when the committed copy is stale.

Classes are the Product Engineering Brief's, plus **S** for credential material, which the brief does
not name but the codebase contains.

- **S** — Secret — must not be in browser storage
- **A** — Authoritative — losing it loses customer work
- **B** — Cache — derived, safe to lose
- **C** — Preference — per-viewer convenience

A key must appear in the table in `scripts/storage-inventory.js` or the test fails. That is the
mechanism that stops an unclassified key being added quietly.


## S — Secret — must not be in browser storage (7)

> Every row here is a **finding**, not a design decision. Browser storage survives the
> tab, the session and the machine's next user. Tracked as WI-10.

| Key | What it holds | Server source of truth | Written by |
|---|---|---|---|
| `anthropic_api_key` | Anthropic API key (legacy key name) | WI-10: server-side proxy | reports-app.js |
| `cygenix_anthropic_key` | Anthropic API key | WI-10: server-side proxy | reports-app.js |
| `cygenix_api_key` | User-supplied API key | WI-10 | balancing.html, connect.html, cygenix-assistant.js +9 |
| `cygenix_conn_string` | Target connection string, in plain text | WI-10: move to a Key Vault reference | connect.html, project-builder-app.js |
| `cygenix_fn_key` | Azure Function key for a user-supplied endpoint | WI-10 | connect.html, project-builder-app.js |
| `cygenix_pending_token` | Auth token in transit between pages | short-lived; clear on consume | index.html |
| `cygenix_src_conn_string` | Source connection string, in plain text | WI-10: move to a Key Vault reference | connect.html, project-builder-app.js |

## A — Authoritative — losing it loses customer work (15)

> These need a server source of truth. `cygenix-cosmos-sync.js` mirrors the keys marked
> *synced* to Cosmos on a 3-second write-behind; the rest are local-only and are WI-1's
> actual scope.

| Key | What it holds | Server source of truth | Written by |
|---|---|---|---|
| `cygenix_backup_history` | Restore-module backup history | NOT SYNCED — WI-1 | dashboard-app.js |
| `cygenix_conv_project` | The currently-open project blob | synced (SYNC_KEYS) | cygenix-cosmos-sync.js, cygenix-schema-graph.js |
| `cygenix_effort_estimates_v1` | Saved Configurator estimates | NOT SYNCED — WI-1 | project_plan.html |
| `cygenix_inventory` | Project artifact inventory | synced (SYNC_KEYS) | dashboard-app.js, projects.html, report.html +1 |
| `cygenix_jobs` | Migration jobs | synced (SYNC_KEYS) | connect.html, cygenix-integrations.js, cygenix-project-summary.js +14 |
| `cygenix_objmap_wip_` | Object-mapping work in progress | NOT SYNCED — WI-1 | schema_explorer.html |
| `cygenix_profiles_v1` | Connection profiles | NOT SYNCED — WI-1 | cygenix-sidebar.js |
| `cygenix_project_connections` | Per-project connection metadata | synced (SYNC_KEYS) | dashboard-app.js |
| `cygenix_project_settings` | Per-project settings | synced (SYNC_KEYS) | dashboard-app.js |
| `cygenix_projects` | The user’s project list | synced (SYNC_KEYS) | agentive_migration.html, cygenix-integrations.js, cygenix-schema-graph.js +5 |
| `cygenix_report` | Generated conversion report | NOT SYNCED — WI-1 | connect.html, dashboard-app.js, project-builder-app.js +2 |
| `cygenix_report_config` | Report configuration | NOT SYNCED — WI-1 | report.html, report_settings.html |
| `cygenix_sys_params` | System parameters | synced (SYNC_KEYS) | dashboard-app.js, system-parameters.html |
| `cygenix_validation_sources` | Validation sources | synced (SYNC_KEYS) | assurance.html |
| `cygenix_wasis_rules` | Was/Is substitution rules | synced (SYNC_KEYS) | project-builder-app.js |

## B — Cache — derived, safe to lose (8)

| Key | What it holds | Server source of truth | Written by |
|---|---|---|---|
| `cygenix_cleansing_handoff` | Hand-off payload between screens | transient | assurance.html |
| `cygenix_insight_` | Cached insight results | recomputed | insights.html, job-editor.html |
| `cygenix_job_scope` | Current job scope selection | recomputed | project-builder-app.js |
| `cygenix_pending_sql` | SQL queued for the editor | transient | data-cleansing.html |
| `cygenix_projects_migrated` | One-time migration marker | idempotent | dashboard-app.js, projects.html |
| `cygenix_ps_ai_` | Cached AI project-summary text | regenerated | dashboard-app.js |
| `cygenix_sql_editor_draft` | Editor draft text | transient | cygenix-assistant-actions.js, data_stream_designer.html, data_stream_store.html |
| `cygenix_wiped_snapshot` | Recovery snapshot after a wipe | recovery aid | cygenix-cosmos-sync.js |

## C — Preference — per-viewer convenience (26)

| Key | What it holds | Server source of truth | Written by |
|---|---|---|---|
| `cyg_goto` | Cross-page navigation intent | — | connect.html, cygenix-sidebar.js, dashboard-app.js +2 |
| `cygenix_active_project` | Legacy project pointer | — | agentive_migration.html, connect.html, cygenix-cosmos-sync.js +2 |
| `cygenix_active_project_id` | Which project is open | a pointer, not the data | agentive_migration.html, cygenix-assistant-actions.js, cygenix-assistant.js +14 |
| `cygenix_active_user` | Signed-in user tag | — | agentive_migration.html, cygenix-cosmos-sync.js, cygenix-sidebar.js +2 |
| `cygenix_analyst_name` | Analyst name for reports | — | projects.html |
| `cygenix_app_prefs` | Theme and app preferences | — | admin.html, agentive_migration.html, assurance.html +33 |
| `cygenix_client_name` | Client name for reports | — | projects.html |
| `cygenix_conn_mode` | Connection mode (direct/function) | — | connect.html |
| `cygenix_current_project_id` | Legacy project pointer | — | data-quality.html |
| `cygenix_entra_account` | MSAL account record | — | agentive_migration.html, connections.js, cygenix-api.js +11 |
| `cygenix_expires` | Session expiry marker | — | cygenix-auth-token.js, login.html, project-manager.js |
| `cygenix_fn_url` | Function URL (not the key) | — | connect.html, project-builder-app.js |
| `cygenix_job_sort` | Job list sort order | — | project-builder-app.js |
| `cygenix_jobs_panel_collapsed` | Jobs panel collapsed | — | project-builder-app.js |
| `cygenix_just_signed_out` | Sign-out flag | — | auth-gate.js, cygenix-sidebar.js, dashboard-app.js +6 |
| `cygenix_onboarded` | Onboarding completed | — | dashboard-app.js, dashboard.html |
| `cygenix_project_name` | Display name of the open project | — | projects.html |
| `cygenix_redirect_after_login` | Post-login destination | — | auth-gate.js, login.html |
| `cygenix_redirect_after_plan` | Post-checkout destination | — | auth-gate.js |
| `cygenix_schemaexp_atlas_smfilters` | Schema Explorer filter state | — | schema_explorer.html |
| `cygenix_sql_panel_collapsed` | SQL panel collapsed | — | sql-editor-app.js |
| `cygenix_src_conn_mode` | Source connection mode | — | connect.html |
| `cygenix_src_fn_url` | Source function URL | — | connect.html |
| `cygenix_token` | Session marker | MSAL holds the real token | admin.html, agentive_migration.html, connect.html +18 |
| `cygenix_user` | Signed-in user (email, name) | — | agentive_migration.html, assurance.html, connect.html +15 |
| `cygenix_user_email` | Signed-in email | — | validation.html |

---

_56 classified key(s) across `public/`._
