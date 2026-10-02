/* tests/browser/one-to-many-open.smoke.js
 * ---------------------------------------------------------------------------
 * Opening a saved job on the standalone one-to-many page (/one-to-many),
 * when the saved table names differ from the database's only in case, or
 * leave out the schema.
 *
 * Oct-2026. The page looked saved names up with ===, so a job saved as
 * "dbo.stg_payor" never found dbo.STG_Payor and the source quietly stayed
 * empty; a target saved as "payor" never picked up its live columns. It now
 * uses the same matching as Object Mapping (public/cygenix-object-names.js).
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/one-to-many-open.smoke.js
 */
'use strict';
const http=require('http'),fs=require('fs'),path=require('path');
const {chromium}=require('playwright-core');
const PUB=path.join(__dirname,'..','..','public');
const P=8483, EXE='/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
let pass=0,fail=0;
const check=(l,ok,x)=>{ok?(pass++,console.log('  PASS  '+l)):(fail++,console.log('  FAIL  '+l+(x?'  → '+String(x).slice(0,300):'')));};
const TYPES={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.svg':'image/svg+xml'};
const ROUTES={};fs.readFileSync(path.join(PUB,'_redirects'),'utf8').split('\n').forEach(l=>{const m=l.trim().match(/^(\/\S*)\s+(\/\S+)\s+200$/);if(m)ROUTES[m[1]]=m[2];});
const server=http.createServer((rq,rs)=>{let p=decodeURIComponent(rq.url.split('?')[0]);if(p==='/')p='/index.html';if(ROUTES[p])p=ROUTES[p];let f=path.join(PUB,p);if(!fs.existsSync(f)&&fs.existsSync(f+'.html'))f+='.html';if(!f.startsWith(PUB)||!fs.existsSync(f)||fs.statSync(f).isDirectory()){rs.writeHead(404);return rs.end('no');}rs.writeHead(200,{'Content-Type':TYPES[path.extname(f)]||'application/octet-stream'});rs.end(fs.readFileSync(f));});
const U='you@example.test';

const SRC={schema:'dbo',name:'STG_Payor',primaryKeys:[],foreignKeys:[],columns:[{name:'PayorID',type:'INT'},{name:'DisplayName',type:'NVARCHAR(50)'}]};
const TGT={schema:'dbo',name:'Payor',primaryKeys:['PayorID'],foreignKeys:[],columns:[{name:'PayorID',type:'INT'},{name:'DisplayName',type:'NVARCHAR(50)'}]};
const TABLES=[SRC,TGT].map(t=>({schema:t.schema,name:t.name,fullName:t.schema+'.'+t.name,type:'BASE TABLE'}));

const job=(id,src,tgts)=>({id,name:id,jobType:'one-to-many',projectId:'p1',created:new Date().toISOString(),
  oneToManyConfig:{srcTable:src,txMode:'all',targetTables:tgts.map(n=>({fullName:n,pkMode:'none',
    mappings:[{srcCol:'displayname',tgtCol:'displayname',transform:'NONE'}],fks:[]}))}});

(async()=>{
  await new Promise(r=>server.listen(P,r));
  const b=await chromium.launch({executablePath:EXE,args:['--no-sandbox']});
  const ctx=await b.newContext({viewport:{width:1500,height:950}});
  const tok='x.'+Buffer.from(JSON.stringify({exp:Math.floor(Date.now()/1000)+3600,preferred_username:U})).toString('base64url')+'.y';
  const json=(r,o)=>r.fulfill({status:200,contentType:'application/json',body:JSON.stringify(o)});
  await ctx.route('**',async r=>{
    const u=r.request().url();
    if(/action=whoami/.test(u))return json(r,{tier:'pro',tier_status:'active',role:'user'});
    if(/db-connect/.test(u)){
      let body={};try{body=JSON.parse(r.request().postData()||'{}');}catch{}
      if(body.action==='schema-tables')return json(r,{success:true,tables:TABLES});
      if(body.action==='schema-columns'){
        const t=[SRC,TGT].find(t=>t.schema===body.schemaName&&t.name===body.tableName);
        return json(r,{success:true,table:t||{columns:[],primaryKeys:[],foreignKeys:[]}});
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
    const conns={srcConnString:'mssql://u:p@src/SRC',tgtConnString:'mssql://u:p@tgt/TGT'};
    localStorage.setItem('cygenix_connections',JSON.stringify(conns));
    const live={};live[a.U]=conns;
    localStorage.setItem('cygenix_project_connections',JSON.stringify(live));
    localStorage.setItem('cygenix_jobs',JSON.stringify(a.JOBS));
  },{U,tok,JOBS:[job('otm_case','dbo.stg_payor',['payor']),job('otm_typo','dbo.STG_Payr',['dbo.Payr'])]});

  const page=await ctx.newPage();
  const errs=[];page.on('pageerror',e=>errs.push(e.message));
  const read=()=>page.evaluate(()=>({
    src:(document.getElementById('src-table-input')||{}).value||'',
    srcCount:(document.getElementById('src-col-count')||{}).textContent||'',
    status:(document.getElementById('status-bar')||{}).textContent||'',
    cards:(typeof targetTables!=='undefined'?targetTables:[]).map(t=>({f:t.fullName,cols:(t.columns||[]).length,m:(t.mappings||[]).map(m=>m.srcCol+'>'+m.tgtCol)})),
  }));

  await page.goto('http://localhost:'+P+'/one-to-many?edit=otm_case',{waitUntil:'domcontentloaded'});
  await page.waitForFunction(()=>/2 cols/.test((document.getElementById('src-col-count')||{}).textContent||''),null,{timeout:15000}).catch(()=>{});
  await page.waitForTimeout(1500);
  const a=await read();
  check('a job saved as "dbo.stg_payor" opens with dbo.STG_Payor as its source, columns loaded',
    a.src==='dbo.STG_Payor'&&/2 cols/.test(a.srcCount),JSON.stringify(a));
  check('its target saved as "payor" is matched to dbo.Payor and picks up the live columns',
    a.cards.length===1&&a.cards[0].f==='dbo.Payor'&&a.cards[0].cols===2,JSON.stringify(a.cards));
  check('the saved mapping row takes the live column spelling',
    a.cards[0]&&a.cards[0].m.indexOf('DisplayName>DisplayName')>=0,JSON.stringify(a.cards));
  check('no error message',!/isn't|Could not match/.test(a.status),a.status);

  await page.goto('http://localhost:'+P+'/one-to-many?edit=otm_typo',{waitUntil:'domcontentloaded'});
  await page.waitForFunction(()=>/Could not match/.test((document.getElementById('status-bar')||{}).textContent||''),null,{timeout:15000}).catch(()=>{});
  const t=await read();
  check('a target that is not there is named, with what to do',
    /Could not match target table in TGT: dbo\.Payr\. Pick it from the Target table list\./.test(t.status),t.status);

  check('no page errors',errs.length===0,errs.join(' | '));
  console.log('\n'+pass+' passed, '+fail+' failed');
  await b.close();server.close();
  process.exit(fail?1:0);
})();
