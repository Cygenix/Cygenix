/* tests/browser/object-map-open.smoke.js
 * ---------------------------------------------------------------------------
 * Opening a saved map, in a real browser.
 *
 * tests/object-map-open.test.js proves the rules. This proves the thing the
 * user actually reported, which no unit test can: click a map created by
 * Conversion Templates and the editor OPENS, with a grid in it.
 *
 * What went wrong. A template-created map is a draft — the template knows
 * which staging table feeds which target table and nothing about the columns,
 * so it saves `columnMapping: []` on purpose. The restore treated that as a
 * broken job, said "No column mapping found in this job" and stopped, leaving
 * both tables loaded, the target's name in the search box, and no grid. It
 * looked exactly like a map that had failed to open.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/object-map-open.smoke.js
 */
'use strict';
const http=require('http'),fs=require('fs'),path=require('path');
const {chromium}=require('playwright-core');
const PUB=path.join(__dirname,'..','..','public');
const P=8479, EXE='/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
let pass=0,fail=0;
const check=(l,ok,x)=>{ok?(pass++,console.log('  PASS  '+l)):(fail++,console.log('  FAIL  '+l+(x?'  → '+String(x).slice(0,300):'')));};
const TYPES={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.svg':'image/svg+xml'};
const ROUTES={};fs.readFileSync(path.join(PUB,'_redirects'),'utf8').split('\n').forEach(l=>{const m=l.trim().match(/^(\/\S*)\s+(\/\S+)\s+200$/);if(m)ROUTES[m[1]]=m[2];});
const server=http.createServer((rq,rs)=>{let p=decodeURIComponent(rq.url.split('?')[0]);if(p==='/')p='/index.html';if(ROUTES[p])p=ROUTES[p];let f=path.join(PUB,p);if(!fs.existsSync(f)&&fs.existsSync(f+'.html'))f+='.html';if(!f.startsWith(PUB)||!fs.existsSync(f)||fs.statSync(f).isDirectory()){rs.writeHead(404);return rs.end('no');}rs.writeHead(200,{'Content-Type':TYPES[path.extname(f)]||'application/octet-stream'});rs.end(fs.readFileSync(f));});
const U='you@example.test';

/* A staging table and the target it feeds. Names chosen so SOME columns match
   exactly and some do not — a grid where everything matched would not show
   that anything had been matched at all. Nothing here names a product: this
   tool is target-agnostic and so is its test data. */
const SRC={schema:'dbo',name:'STG_Payor',primaryKeys:['PayorID'],foreignKeys:[],columns:[
  {name:'PayorID',type:'UNIQUEIDENTIFIER',nullable:false},
  {name:'PayorIndex',type:'INT',nullable:true},
  {name:'Client',type:'INT',nullable:true},
  {name:'DisplayName',type:'NVARCHAR(127)',nullable:true},
  {name:'LegacyRef',type:'NVARCHAR(40)',nullable:true}]};
const TGT={schema:'dbo',name:'Payor',primaryKeys:['PayorID'],foreignKeys:[],columns:[
  {name:'RowId',type:'INT',nullable:false,isIdentity:true},
  {name:'PayorID',type:'UNIQUEIDENTIFIER',nullable:false},
  {name:'PayorIndex',type:'INT',nullable:true},
  {name:'Client',type:'INT',nullable:true},
  {name:'DisplayName',type:'NVARCHAR(127)',nullable:true},
  {name:'OpenedOn',type:'DATETIME',nullable:true}]};
// A second schema holding a table of the same name, so a saved name with no
// schema ("STG_PAYOR") fits two tables and the page has to ask.
const SRC2={schema:'stg',name:'STG_Payor',primaryKeys:[],foreignKeys:[],columns:[{name:'PayorID',type:'INT',nullable:true}]};
const TABLES=[SRC,TGT,SRC2].map(t=>({schema:t.schema,name:t.name,fullName:t.schema+'.'+t.name,type:'BASE TABLE'}));
// Flipped by the "source disconnected" case below.
let SRC_DOWN=false;

const JOB_ID='job_tpl_1789722047112_ausrq7';

(async()=>{
  await new Promise(r=>server.listen(P,r));
  const b=await chromium.launch({executablePath:EXE,args:['--no-sandbox']});
  const ctx=await b.newContext({viewport:{width:1500,height:950}});
  const tok='x.'+Buffer.from(JSON.stringify({exp:Math.floor(Date.now()/1000)+3600,preferred_username:U})).toString('base64url')+'.y';
  const CALLS=[];
  const json=(r,o)=>r.fulfill({status:200,contentType:'application/json',body:JSON.stringify(o)});

  await ctx.route('**',async r=>{
    const u=r.request().url();
    if(/action=whoami/.test(u))return json(r,{tier:'pro',tier_status:'active',role:'user'});
    // Anything reaching api.anthropic.com is a bug in this test's premise:
    // opening a saved map must never call Claude.
    if(/api\.anthropic\.com/.test(u)){CALLS.push('anthropic');return json(r,{});}
    if(/db-connect/.test(u)){
      let body={};try{body=JSON.parse(r.request().postData()||'{}');}catch{}
      CALLS.push(body.action||'?');
      if(SRC_DOWN&&/@src\//.test(JSON.stringify(body)))return r.fulfill({status:500,contentType:'application/json',body:JSON.stringify({success:false,error:'Login failed for user'})});
      if(body.action==='schema-tables')return json(r,{success:true,tables:TABLES});
      if(body.action==='schema-columns'){
        const t=[SRC,TGT,SRC2].find(t=>t.schema===body.schemaName&&t.name===body.tableName);
        return json(r,{success:true,table:t||{schema:body.schemaName,name:body.tableName,columns:[],primaryKeys:[],foreignKeys:[]}});
      }
      return json(r,{success:true});
    }
    if(u.startsWith('http://localhost:'+P))return r.continue();
    return json(r,{});
  });

  await ctx.addInitScript(a=>{
    [localStorage,sessionStorage].forEach(s=>{s.setItem('cygenix_token',a.tok);s.setItem('cygenix_expires',String(Date.now()+36e5));});
    localStorage.setItem('cygenix_onboarded','true');
    localStorage.setItem('cygenix_user',JSON.stringify({email:a.U}));
    localStorage.setItem('cygenix_active_user',a.U);
    localStorage.setItem('cygenix_tier','pro');
    localStorage.setItem('cygenix_cookie_consent',JSON.stringify({version:'2',essential:true,functional:true,analytics:false,timestamp:new Date().toISOString()}));
    const acct={homeAccountId:'h.t',environment:'cygenix.ciamlogin.com',tenantId:'t',username:a.U,localAccountId:'l',authorityType:'MSSTS',name:'You'};
    localStorage.setItem('acct-cygenix.ciamlogin.com-h.t',JSON.stringify(acct));
    localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
      JSON.stringify({credentialType:'IdToken',secret:a.tok,expiresOn:String(Math.floor(Date.now()/1000)+3600)}));
    localStorage.setItem('cygenix_projects',JSON.stringify([{id:'p1',name:'Acme'}]));
    localStorage.setItem('cygenix_active_project_id','p1');
    // An API key IS set. The user who reported this had one — that is how they
    // got as far as "Remap error" — so the check that opening a map does not
    // call Claude has to run with a key present, or it proves nothing.
    localStorage.setItem('cygenix_api_key','sk-ant-test-not-a-real-key');
    const conns={srcConnString:'mssql://u:p@src/SRC',tgtConnString:'mssql://u:p@tgt/TGT'};
    localStorage.setItem('cygenix_connections',JSON.stringify(conns));
    const live={};live[a.U]=conns;
    localStorage.setItem('cygenix_project_connections',JSON.stringify(live));
    // The job exactly as cygenix-template-mapping.js writes it: a pair, a
    // stamp, and NO columns.
    localStorage.setItem('cygenix_jobs',JSON.stringify([{
      id:a.JOB_ID,name:'STG_Payor → Payor',jobType:'simple-map',projectId:'p1',
      source:'dbo.STG_Payor',sourceTable:'dbo.STG_Payor',
      target:'dbo.Payor',targetTable:'dbo.Payor',
      columnMapping:[],status:'draft',created:new Date().toISOString(),
      fromTemplate:{templateId:'tpl_1',module:'Payors',version:1,tableId:'t1'}
    },{
      // A map somebody actually built. Seeded here rather than added from the
      // page, because addInitScript runs again on every navigation and would
      // put this list back the way it found it.
      id:'job_real',name:'Real map',jobType:'simple-map',projectId:'p1',
      source:'dbo.STG_Payor',sourceTable:'dbo.STG_Payor',
      target:'dbo.Payor',targetTable:'dbo.Payor',
      columnMapping:[{srcCol:'LegacyRef',tgtCol:'DisplayName',transform:'TRIM',match:'LOW'}],
      created:new Date().toISOString()
    },{
      // Oct-2026: saved in a different case from the database, with the
      // target's schema missing, and its columns in the wrong case too.
      id:'job_case',name:'stg_payor → payor',jobType:'simple-map',projectId:'p1',
      source:'dbo.stg_payor',sourceTable:'dbo.stg_payor',
      target:'PAYOR',targetTable:'PAYOR',
      columnMapping:[{srcCol:'legacyref',tgtCol:'displayname',transform:'TRIM',match:'LOW'}],
      created:new Date().toISOString()
    },{
      id:'job_typo',name:'typo',jobType:'simple-map',projectId:'p1',
      source:'dbo.STG_Payr',sourceTable:'dbo.STG_Payr',target:'dbo.Payor',targetTable:'dbo.Payor',
      columnMapping:[],created:new Date().toISOString()
    },{
      id:'job_ambig',name:'ambiguous',jobType:'simple-map',projectId:'p1',
      source:'STG_PAYOR',sourceTable:'STG_PAYOR',target:'dbo.Payor',targetTable:'dbo.Payor',
      columnMapping:[],created:new Date().toISOString()
    }]));
  },{U,tok,JOB_ID});

  const page=await ctx.newPage();
  const errs=[];page.on('pageerror',e=>errs.push(e.message));
  page.on('console',m=>{
    if(m.type()!=='error')return;
    const t=m.text();
    if(/Failed to load resource|CygenixSync|favicon|fonts\./.test(t))return;
    errs.push('console: '+t);
  });
  page.on('dialog',d=>d.accept());

  await page.goto('http://localhost:'+P+'/object-mapping?edit='+JOB_ID,{waitUntil:'domcontentloaded'});

  /* ── It opens ─────────────────────────────────────────────────────────── */
  const opened=await page.waitForFunction(()=>{
    const w=document.getElementById('mapping-wrap');
    return !!w && w.style.display==='block' && document.querySelectorAll('#mapping-tbody tr').length>0;
  },null,{timeout:25000}).then(()=>true).catch(()=>false);
  check('a map with no columns mapped yet OPENS — the grid is on screen',opened,
    await page.evaluate(()=>({status:(document.getElementById('status-bar')||{}).textContent,
      wrap:(document.getElementById('mapping-wrap')||{}).style?.display,
      rows:document.querySelectorAll('#mapping-tbody tr').length})).then(JSON.stringify));

  const state=await page.evaluate(()=>({
    banner:(document.getElementById('edit-job-name')||{}).textContent||'',
    status:(document.getElementById('status-bar')||{}).textContent||'',
    statusClass:(document.getElementById('status-bar')||{}).className||'',
    rows:Array.from(document.querySelectorAll('#mapping-tbody tr')).length,
    // Read the GRID, not the variable. `columnMapping` is a module-level let
    // in a plain <script src> — it is not on window, and reaching for it
    // returns undefined and quietly passes every check that follows.
    mapping:Array.from(document.querySelectorAll('#mapping-tbody tr')).map(tr=>{
      // Phase 4 column order: Target column · Source column · Transform ·
      // Fixed value · Confidence. The target cell's first element is the
      // lineage span carrying the bare column name; the type and any NOT
      // NULL note follow it.
      const tds=tr.children;
      const sel=tds[1].querySelector('select');
      return [tds[0].firstElementChild.textContent.trim(), sel?sel.value:''];
    }),
    stats:(document.getElementById('map-stats')||{}).textContent||'',
  }));

  check('every target column is a row, identity included',
    state.mapping.map(m=>m[0]).join(',')==='PayorID,PayorIndex,Client,DisplayName,OpenedOn,RowId'
    || state.mapping.length===6, JSON.stringify(state.mapping));

  check('the columns whose names match are matched, and the others left blank',
    (()=>{const by=Object.fromEntries(state.mapping);
      return by.PayorID==='PayorID'&&by.PayorIndex==='PayorIndex'&&by.Client==='Client'
        &&by.DisplayName==='DisplayName'&&!by.OpenedOn;})(),JSON.stringify(state.mapping));

  check('the source column with no home stays out of it rather than being forced somewhere',
    !state.mapping.some(m=>m[1]==='LegacyRef'),JSON.stringify(state.mapping));

  check('the banner still says which map is being edited',
    /STG_Payor → Payor/.test(state.banner),state.banner);

  check('it SAYS it matched them by name, and that nothing is saved yet',
    /by name/.test(state.status)&&/Save as job/.test(state.status),state.status);

  check('…as a warning that stays put, not an error and not a flash',
    /status-warn/.test(state.statusClass)&&!/status-err/.test(state.statusClass),state.statusClass);

  check('the old dead end is gone — no "No column mapping found"',
    !/No column mapping found/.test(state.status),state.status);

  /* ── And it did it without spending anything ──────────────────────────── */
  check('opening the map never called Claude',
    CALLS.indexOf('anthropic')===-1,CALLS.join(','));

  check('it read the schema the ordinary way — no new backend action',
    CALLS.every(c=>['schema-tables','schema-columns','schema','test','?'].indexOf(c)>=0),CALLS.join(','));

  /* ── A saved map with columns still restores those columns ────────────── */
  await page.goto('http://localhost:'+P+'/object-mapping?edit=job_real',{waitUntil:'domcontentloaded'});
  await page.waitForFunction(()=>{
    const w=document.getElementById('mapping-wrap');
    return !!w&&w.style.display==='block'&&document.querySelectorAll('#mapping-tbody tr').length>0;
  },null,{timeout:25000}).catch(()=>{});
  const real=await page.evaluate(()=>({
    mapping:Array.from(document.querySelectorAll('#mapping-tbody tr')).map(tr=>{
      const tds=tr.children;
      const sel=tds[1].querySelector('select');
      const tf=tds[2].querySelector('select');
      return [tds[0].firstElementChild.textContent.trim(), sel?sel.value:'', tf?tf.value:''];
    }),
    status:(document.getElementById('status-bar')||{}).textContent||'',
  }));
  check('a map that HAS columns still restores exactly what was saved, untouched',
    real.mapping.some(m=>m[0]==='DisplayName'&&m[1]==='LegacyRef'&&m[2]==='TRIM'),
    JSON.stringify(real.mapping));
  check('…and it is not told it was matched by name, because it was not',
    !/by name/.test(real.status),real.status);

  /* ── Oct-2026: names saved in another case, or with no schema ─────────── */
  const openAndWait=async(id)=>{
    await page.goto('http://localhost:'+P+'/object-mapping?edit='+id,{waitUntil:'domcontentloaded'});
    await page.waitForFunction(()=>{
      const w=document.getElementById('mapping-wrap'), s=document.getElementById('status-bar');
      return (!!w&&w.style.display==='block'&&document.querySelectorAll('#mapping-tbody tr').length>0)
        || (s&&/status-err/.test(s.className));
    },null,{timeout:25000}).catch(()=>{});
    await page.waitForTimeout(300);
  };
  const grid=()=>page.evaluate(()=>({
    mapping:Array.from(document.querySelectorAll('#mapping-tbody tr')).map(tr=>{
      const tds=tr.children; const sel=tds[1].querySelector('select'); const tf=tds[2].querySelector('select');
      return [tds[0].firstElementChild.textContent.trim(), sel?sel.value:'', tf?tf.value:''];
    }),
    wrap:(document.getElementById('mapping-wrap')||{}).style?.display,
    status:(document.getElementById('status-bar')||{}).textContent||'',
    statusClass:(document.getElementById('status-bar')||{}).className||'',
    src:(document.getElementById('src-table-input')||{}).value||'',
    tgt:(document.getElementById('tgt-table-input')||{}).value||'',
    picks:Array.from(document.querySelectorAll('#status-bar .om-pick-row button')).map(b=>b.textContent),
  }));

  await openAndWait('job_case');
  const cs=await grid();
  check('a map saved as "dbo.stg_payor" → "PAYOR" OPENS against dbo.STG_Payor → dbo.Payor',
    cs.wrap==='block'&&cs.mapping.length>0&&!/status-err/.test(cs.statusClass),JSON.stringify({status:cs.status,src:cs.src,tgt:cs.tgt}));
  check('…and its saved column row, also in the wrong case, is restored onto the live columns',
    cs.mapping.some(m=>m[0]==='DisplayName'&&m[1]==='LegacyRef'&&m[2]==='TRIM')
    && cs.mapping.filter(m=>m[0].toLowerCase()==='displayname').length===1,JSON.stringify(cs.mapping));
  check('…with no "not found" and no "reconnect"',!/not found|reconnect/i.test(cs.status),cs.status);
  // Save it, the ordinary way, and read back what was written.
  await page.evaluate(()=>{ try{ saveAsJob(); }catch(e){} });
  await page.waitForTimeout(800);
  const savedCase=await page.evaluate(()=>(JSON.parse(localStorage.getItem('cygenix_jobs')||'[]').find(j=>j.id==='job_case')||{}));
  check('saving it writes the live spelling — dbo.STG_Payor → dbo.Payor, DisplayName ← LegacyRef',
    savedCase.sourceTable==='dbo.STG_Payor'&&savedCase.targetTable==='dbo.Payor'
    &&(savedCase.columnMapping||[]).some(m=>m.tgtCol==='DisplayName'&&m.srcCol==='LegacyRef'),
    JSON.stringify({s:savedCase.sourceTable,t:savedCase.targetTable,m:savedCase.columnMapping}));

  await openAndWait('job_typo');
  const ty=await grid();
  check('a table that is not there says it is not in SRC, and to pick it from the Source table list',
    /Table "dbo\.STG_Payr" isn't in SRC\. Pick it from the Source table list\./.test(ty.status),ty.status);
  check('…names the close match',/Close matches: .*dbo\.STG_Payor/.test(ty.status),ty.status);
  check('…and does NOT say reconnect, because the connection is fine',!/reconnect/i.test(ty.status),ty.status);

  await openAndWait('job_ambig');
  await page.waitForFunction(()=>document.querySelectorAll('#status-bar .om-pick-row button').length>0,null,{timeout:15000}).catch(()=>{});
  const am=await grid();
  check('"STG_PAYOR" fits dbo.STG_Payor and stg.STG_Payor: the page asks, listing both, and opens neither',
    /matches more than one source table/.test(am.status)&&am.picks.indexOf('dbo.STG_Payor')>=0&&am.picks.indexOf('stg.STG_Payor')>=0
    &&am.wrap!=='block',JSON.stringify({status:am.status,picks:am.picks}));
  await page.evaluate(()=>{ const b=Array.from(document.querySelectorAll('#status-bar .om-pick-row button')).find(b=>b.textContent==='dbo.STG_Payor'); if(b) b.click(); });
  await page.waitForFunction(()=>{const w=document.getElementById('mapping-wrap');return !!w&&w.style.display==='block';},null,{timeout:15000}).catch(()=>{});
  const am2=await grid();
  check('picking dbo.STG_Payor opens the map on it',am2.wrap==='block'&&am2.src==='dbo.STG_Payor'&&am2.picks.length===0,JSON.stringify({src:am2.src,status:am2.status}));

  SRC_DOWN=true;
  await openAndWait('job_real');
  const dn=await grid();
  SRC_DOWN=false;
  check('with the source connection down, the message still says "Reconnect source DB"',
    /Reconnect source DB/.test(dn.status)&&/status-err/.test(dn.statusClass),dn.status);

  check('no page errors',errs.length===0,errs.join(' | '));

  console.log('\n'+pass+' passed, '+fail+' failed');
  await b.close();server.close();
  process.exit(fail?1:0);
})();
