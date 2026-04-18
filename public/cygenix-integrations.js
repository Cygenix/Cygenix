/* cygenix-integrations.js — integration registry + connector handlers
 *
 * Architecture:
 *   CygenixIntegrations.list()          → connector metadata array
 *   CygenixIntegrations.getConfig(id)   → saved config for a connector (or null)
 *   CygenixIntegrations.saveConfig(id, cfg)
 *   CygenixIntegrations.isEnabled(id)   → bool
 *   CygenixIntegrations.setEnabled(id, on)
 *   CygenixIntegrations.test(id)        → test handler, returns { ok, message }
 *   CygenixIntegrations.run(id, payload) → run handler, returns { ok, message, data? }
 *   CygenixIntegrations.emit(event, payload)  → broadcast to all enabled subscribers (layer 2)
 *
 * All state lives in localStorage at `cygenix_integrations`. Credentials stored
 * there are browser-local — NOT a long-term secrets solution. Surface this in UI.
 */
(function(){
  const STORAGE_KEY = 'cygenix_integrations';

  // ─── Registry ────────────────────────────────────────────────────────────
  // Each connector has: id, name, description, category, icon, status, handlers
  // status: 'available' (working) | 'coming-soon' (placeholder)
  // handlers: { configFields, test, run } — present only for available connectors
  const CONNECTORS = [
    // ── Available — real, working today ─────────────────────────────────
    {
      id: 'webhook',
      name: 'Webhook',
      description: 'POST JSON payloads to any URL. Use it to signal external systems when migrations run.',
      category: 'notifications',
      icon: '🪝',
      status: 'available',
      configFields: [
        { key: 'url',     label: 'Webhook URL',  type: 'url',    placeholder: 'https://your-service.example.com/hook' },
        { key: 'secret',  label: 'Optional secret (sent as X-Cygenix-Signature header)', type: 'password', placeholder: 'shared secret' }
      ]
    },
    {
      id: 'json-export',
      name: 'JSON project export',
      description: 'Export the active project (settings, groups, jobs, mappings) as a single JSON bundle.',
      category: 'export',
      icon: '📤',
      status: 'available',
      configFields: []
    },
    {
      id: 'json-import',
      name: 'JSON project import',
      description: 'Read a Cygenix JSON bundle from your computer and create / replace a project from it.',
      category: 'import',
      icon: '📥',
      status: 'available',
      configFields: []
    },
    {
      id: 'csv-jobs',
      name: 'Jobs CSV export',
      description: 'Export the jobs library as a CSV file, suitable for spreadsheet review or hand-off.',
      category: 'export',
      icon: '📊',
      status: 'available',
      configFields: []
    },

    // ── Coming soon — honest placeholders ──────────────────────────────
    { id: 'adf',       name: 'Azure Data Factory', description: 'Trigger ADF pipelines from Cygenix events.',   category: 'etl',          icon: '🏭', status: 'coming-soon' },
    { id: 'ssis',      name: 'SQL Server Integration Services', description: 'Kick off SSIS packages after migration.', category: 'etl',          icon: '📦', status: 'coming-soon' },
    { id: 'fivetran',  name: 'Fivetran',           description: 'Orchestrate Fivetran connectors around conversion runs.', category: 'etl',   icon: '🚚', status: 'coming-soon' },
    { id: 'airbyte',   name: 'Airbyte',            description: 'Trigger Airbyte syncs as pipeline steps.',     category: 'etl',          icon: '🌀', status: 'coming-soon' },
    { id: 'dbt',       name: 'dbt Cloud',          description: 'Run dbt jobs after migration completes.',      category: 'etl',          icon: '🧱', status: 'coming-soon' },
    { id: 'slack',     name: 'Slack',              description: 'Post migration status updates to a Slack channel.', category: 'notifications', icon: '💬', status: 'coming-soon' },
    { id: 'teams',     name: 'Microsoft Teams',    description: 'Post migration events to a Teams channel.',    category: 'notifications', icon: '👥', status: 'coming-soon' },
    { id: 'jira',      name: 'Jira',               description: 'File issues automatically on migration failure.',category: 'workflow',     icon: '🎯', status: 'coming-soon' },
    { id: 'azdo',      name: 'Azure DevOps',       description: 'Create work items for failed validations.',     category: 'workflow',     icon: '🔷', status: 'coming-soon' },
    { id: 'github',    name: 'GitHub',             description: 'Commit generated SQL scripts to a repository.',  category: 'workflow',     icon: '🐙', status: 'coming-soon' },
    { id: 'smtp',      name: 'Email (SMTP)',       description: 'Email notifications on migration completion.',  category: 'notifications', icon: '📧', status: 'coming-soon' },
    { id: 's3',        name: 'AWS S3',             description: 'Upload conversion reports to S3 buckets.',     category: 'storage',       icon: '🪣', status: 'coming-soon' },
    { id: 'blob',      name: 'Azure Blob Storage', description: 'Archive migration bundles to Azure Blob.',     category: 'storage',       icon: '📦', status: 'coming-soon' },
    { id: 'sharepoint',name: 'SharePoint',         description: 'Push reports to a SharePoint document library.',category: 'storage',      icon: '📁', status: 'coming-soon' },
  ];

  // ─── Storage ─────────────────────────────────────────────────────────────
  function loadAll(){
    try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}') || {}; }
    catch { return {}; }
  }
  function saveAll(obj){
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(obj)); } catch {}
  }
  function getConfig(id){
    const all = loadAll();
    return all[id] || null;
  }
  function saveConfig(id, cfg){
    const all = loadAll();
    all[id] = { ...(all[id]||{}), ...cfg };
    saveAll(all);
    return all[id];
  }
  function isEnabled(id){
    const c = getConfig(id);
    return !!(c && c.enabled);
  }
  function setEnabled(id, on){
    saveConfig(id, { enabled: !!on });
  }

  // ─── Handlers ────────────────────────────────────────────────────────────
  // Each handler returns { ok: bool, message: string, data?: any }

  async function testWebhook(cfg){
    if (!cfg || !cfg.url) return { ok:false, message:'Webhook URL not set' };
    const payload = {
      event: 'cygenix.webhook.test',
      sentAt: new Date().toISOString(),
      message: 'This is a test payload from Cygenix. Receiving this means your webhook works.'
    };
    try {
      const headers = { 'Content-Type':'application/json' };
      if (cfg.secret) headers['X-Cygenix-Signature'] = cfg.secret;
      // Direct fetch. Will fail with CORS against most endpoints — UI flags this.
      const res = await fetch(cfg.url, { method:'POST', headers, body: JSON.stringify(payload), mode:'cors' });
      if (!res.ok) return { ok:false, message:'HTTP '+res.status+' '+res.statusText };
      return { ok:true, message:'✓ POST succeeded ('+res.status+')' };
    } catch(e){
      // Most common failure: CORS blocked. Be honest.
      return { ok:false, message:'Request failed: '+(e.message||'network error')+' — most webhook services block cross-origin browser requests. If you see this, your endpoint either (a) is down, or (b) needs CORS headers, or (c) needs a server-side relay.' };
    }
  }

  async function runWebhook(cfg, payload){
    if (!cfg || !cfg.url) return { ok:false, message:'Webhook URL not set' };
    try {
      const headers = { 'Content-Type':'application/json' };
      if (cfg.secret) headers['X-Cygenix-Signature'] = cfg.secret;
      const res = await fetch(cfg.url, { method:'POST', headers, body: JSON.stringify(payload), mode:'cors' });
      return { ok: res.ok, message: res.ok ? 'delivered' : 'HTTP '+res.status };
    } catch(e){
      return { ok:false, message: e.message || 'network error' };
    }
  }

  // JSON export — no config needed; run builds the bundle and triggers download
  function runJsonExport(){
    try {
      const projects = JSON.parse(localStorage.getItem('cygenix_projects') || '[]');
      const activeId = localStorage.getItem('cygenix_active_project_id') || '';
      const project  = projects.find(p => p.id === activeId) || projects[0];
      if (!project) return { ok:false, message:'No active project to export' };

      const jobs     = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]');
      const projectJobs = jobs.filter(j => (j.projectId||'') === project.id);

      const bundle = {
        cygenixVersion: 1,
        exportedAt:     new Date().toISOString(),
        kind:           'cygenix-project-bundle',
        project,
        jobs: projectJobs,
        note: 'This bundle contains one project and its jobs. Import via Integrations → JSON project import.'
      };

      const filename = 'cygenix_' + (project.name || 'project').replace(/[^a-z0-9_\-]+/gi,'_') + '_' + new Date().toISOString().slice(0,10) + '.json';
      const blob = new Blob([JSON.stringify(bundle, null, 2)], { type:'application/json' });
      const url  = URL.createObjectURL(blob);
      const a    = document.createElement('a');
      a.href = url; a.download = filename;
      document.body.appendChild(a); a.click(); document.body.removeChild(a);
      URL.revokeObjectURL(url);
      return { ok:true, message:'Exported ' + filename + ' ('+projectJobs.length+' job'+(projectJobs.length===1?'':'s')+')' };
    } catch(e){
      return { ok:false, message:'Export failed: '+e.message };
    }
  }

  // JSON import — takes parsed bundle, returns a result. UI handles file reading.
  function runJsonImport(bundle, mode){
    // mode: 'new' (create new project) | 'replace' (replace current active)
    if (!bundle || bundle.kind !== 'cygenix-project-bundle' || !bundle.project){
      return { ok:false, message:'Not a Cygenix project bundle (missing project or wrong kind)' };
    }
    try {
      const projects = JSON.parse(localStorage.getItem('cygenix_projects') || '[]');
      const jobs     = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]');

      const imported = bundle.project;
      let newProjectId;

      if (mode === 'replace'){
        const activeId = localStorage.getItem('cygenix_active_project_id') || '';
        if (!activeId) return { ok:false, message:'No active project to replace' };
        // Keep the active id; replace its fields with imported
        const idx = projects.findIndex(p => p.id === activeId);
        if (idx < 0) return { ok:false, message:'Active project not found' };
        projects[idx] = { ...imported, id: activeId, name: imported.name || projects[idx].name };
        newProjectId = activeId;
        // Remove any existing jobs for this project to avoid duplicates
        const remaining = jobs.filter(j => j.projectId !== activeId);
        const importedJobs = (bundle.jobs||[]).map(j => ({ ...j, projectId: activeId, id: j.id || ('job_'+Date.now()+'_'+Math.random().toString(36).slice(2,6)) }));
        localStorage.setItem('cygenix_jobs', JSON.stringify([...importedJobs, ...remaining]));
      } else {
        // Create a new project with a fresh id
        newProjectId = 'proj_' + Date.now() + '_' + Math.random().toString(36).slice(2,6);
        const newProject = { ...imported, id: newProjectId, name: (imported.name||'Imported project') + ' (imported)', createdAt: new Date().toISOString() };
        projects.push(newProject);
        const importedJobs = (bundle.jobs||[]).map(j => ({ ...j, projectId: newProjectId, id: ('job_'+Date.now()+'_'+Math.random().toString(36).slice(2,6)) }));
        localStorage.setItem('cygenix_jobs', JSON.stringify([...importedJobs, ...jobs]));
      }

      localStorage.setItem('cygenix_projects', JSON.stringify(projects));
      localStorage.setItem('cygenix_active_project_id', newProjectId);
      return { ok:true, message:'Imported '+(bundle.jobs?.length||0)+' job(s). Switch to project-builder to see the imported project.' };
    } catch(e){
      return { ok:false, message:'Import failed: '+e.message };
    }
  }

  function runCsvJobs(){
    try {
      const jobs = JSON.parse(localStorage.getItem('cygenix_jobs') || '[]');
      const projects = JSON.parse(localStorage.getItem('cygenix_projects') || '[]');
      const projName = (id) => (projects.find(p=>p.id===id)?.name) || '';
      if (!jobs.length) return { ok:false, message:'No jobs to export' };

      const q = v => '"' + String(v==null?'':v).replace(/"/g,'""').replace(/\r?\n/g, ' ') + '"';
      const cols = ['id','name','projectId','projectName','jobType','type','source','target','status','created','totalRows','mappedColumns'];
      const header = cols.map(q).join(',');
      const rows = jobs.map(j => {
        const mapped = (j.columnMapping||[]).filter(m => m && m.tgtCol).length;
        return [
          j.id, j.name, j.projectId||'', projName(j.projectId),
          j.jobType||'', j.type||'', j.source||'', j.target||'',
          j.status||'', j.created||'', j.totalRows||0, mapped
        ].map(q).join(',');
      });
      const csv = [header, ...rows].join('\n');

      const filename = 'cygenix_jobs_' + new Date().toISOString().slice(0,10) + '.csv';
      const blob = new Blob([csv], { type:'text/csv' });
      const url  = URL.createObjectURL(blob);
      const a    = document.createElement('a');
      a.href = url; a.download = filename;
      document.body.appendChild(a); a.click(); document.body.removeChild(a);
      URL.revokeObjectURL(url);
      return { ok:true, message:'Exported '+jobs.length+' job(s) to '+filename };
    } catch(e){
      return { ok:false, message:'CSV export failed: '+e.message };
    }
  }

  // ─── Dispatch ────────────────────────────────────────────────────────────
  async function test(id){
    const c = CONNECTORS.find(c => c.id === id);
    if (!c) return { ok:false, message:'Unknown connector' };
    if (c.status !== 'available') return { ok:false, message:'Connector not yet implemented' };
    const cfg = getConfig(id) || {};
    if (id === 'webhook')     return testWebhook(cfg);
    // Most connectors have no meaningful test (export/import need user action)
    return { ok:true, message:'No test available — use Run instead.' };
  }

  async function run(id, payload){
    const c = CONNECTORS.find(c => c.id === id);
    if (!c) return { ok:false, message:'Unknown connector' };
    if (c.status !== 'available') return { ok:false, message:'Connector not yet implemented' };
    const cfg = getConfig(id) || {};
    switch (id){
      case 'webhook':      return runWebhook(cfg, payload || {event:'cygenix.manual.trigger', sentAt:new Date().toISOString()});
      case 'json-export':  return runJsonExport();
      case 'json-import':  return runJsonImport(payload?.bundle, payload?.mode || 'new');
      case 'csv-jobs':     return runCsvJobs();
      default:             return { ok:false, message:'No handler for '+id };
    }
  }

  // Layer 2 stub: broadcast an event to all enabled subscribers.
  // Today this only calls webhook if enabled; future connectors plug in here.
  async function emit(event, payload){
    const out = { event, sentAt: new Date().toISOString(), ...payload };
    const results = [];
    if (isEnabled('webhook')){
      results.push({ id:'webhook', ...(await runWebhook(getConfig('webhook'), out)) });
    }
    return results;
  }

  window.CygenixIntegrations = {
    list: () => CONNECTORS.slice(),
    getConnector: (id) => CONNECTORS.find(c => c.id === id) || null,
    getConfig, saveConfig, isEnabled, setEnabled,
    test, run, emit
  };
})();
