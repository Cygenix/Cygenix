/* tests/browser/data-generator.smoke.js
 * ---------------------------------------------------------------------------
 * The reworked Data Generator, in a real browser. Phase 1: it reads a schema
 * and plans; it does not write.
 *
 * tests/data-generator.test.js proves the rules. This walks the things only a
 * browser can answer: that the page boots with no demo tables, that the picker
 * lists what the source actually has, that choosing a child pulls its parents
 * in, that the insert order appears, that the structure is read-only and the
 * preview shows the parent an FK points at — and that nothing at all is
 * written to the stub database along the way.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/data-generator.smoke.js
 */
'use strict';
const http=require('http'),fs=require('fs'),path=require('path');
const {chromium}=require('playwright-core');
const PUB=path.join(__dirname,'..','..','public');
const P=Number(process.env.SMOKE_PORT||8481);
const EXE=process.env.CHROMIUM||'/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
let pass=0,fail=0;
const check=(l,ok,x)=>{ok?(pass++,console.log('  PASS  '+l)):(fail++,console.log('  FAIL  '+l+(x?'  → '+String(x).slice(0,300):'')));};
const TYPES={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.svg':'image/svg+xml'};
const ROUTES={};fs.readFileSync(path.join(PUB,'_redirects'),'utf8').split('\n').forEach(l=>{const m=l.trim().match(/^(\/\S*)\s+(\/\S+)\s+200$/);if(m)ROUTES[m[1]]=m[2];});
const server=http.createServer((rq,rs)=>{let q=decodeURIComponent(rq.url.split('?')[0]);if(q==='/')q='/index.html';if(ROUTES[q])q=ROUTES[q];let f=path.join(PUB,q);if(!fs.existsSync(f)&&fs.existsSync(f+'.html'))f+='.html';if(!f.startsWith(PUB)||!fs.existsSync(f)||fs.statSync(f).isDirectory()){rs.writeHead(404);return rs.end('no');}rs.writeHead(200,{'Content-Type':TYPES[path.extname(f)]||'application/octet-stream'});rs.end(fs.readFileSync(f));});
const U='you@example.test';

/* A stub source: three tables in two schemas, a chain of foreign keys, one
   identity column and one computed one. Every call it receives is recorded,
   so "nothing was written" is a fact rather than a hope. */
const CALLS=[];
const TABLES=[
 {schema:'dbo',name:'Client',kind:'table',rowCount:12},
 {schema:'dbo',name:'Matter',kind:'table',rowCount:340},
 {schema:'fin',name:'Ledger',kind:'table',rowCount:9001},
];
const FKS=[
 {fromSchema:'dbo',fromTable:'Matter',fromColumn:'ClientId',toSchema:'dbo',toTable:'Client',toColumn:'Id',name:'FK_M_C'},
];
const COLS={
 'dbo.Client':{schema:'dbo',name:'Client',primaryKeys:['Id'],uniques:[{name:'UQ_Code',columns:['Code']}],
   foreignKeys:[],columns:[
   {name:'Id',type:'INT',baseType:'int',nullable:false,isIdentity:true,ordinal:1},
   {name:'Code',type:'NVARCHAR(10)',baseType:'nvarchar',maxLength:10,nullable:false,ordinal:2},
   {name:'Email',type:'NVARCHAR(50)',baseType:'nvarchar',maxLength:50,nullable:true,ordinal:3},
   {name:'Display',type:'NVARCHAR(80)',baseType:'nvarchar',nullable:true,isComputed:true,ordinal:4}]},
 'dbo.Matter':{schema:'dbo',name:'Matter',primaryKeys:['Id'],uniques:[],foreignKeys:[],columns:[
   {name:'Id',type:'INT',baseType:'int',nullable:false,isIdentity:true,ordinal:1},
   {name:'ClientId',type:'INT',baseType:'int',nullable:false,ordinal:2},
   {name:'Title',type:'NVARCHAR(200)',baseType:'nvarchar',maxLength:200,nullable:true,ordinal:3}]},
 'fin.Ledger':{schema:'fin',name:'Ledger',primaryKeys:['Id'],uniques:[],foreignKeys:[],columns:[
   {name:'Id',type:'INT',baseType:'int',nullable:false,isIdentity:true,ordinal:1},
   {name:'Amount',type:'DECIMAL(9,2)',baseType:'decimal',precision:9,scale:2,nullable:false,ordinal:2}]},
};

(async()=>{
 await new Promise(r=>server.listen(P,r));
 const b=await chromium.launch({executablePath:EXE,args:['--no-sandbox']});
 const ctx=await b.newContext({viewport:{width:1500,height:1000}});
 const tok='x.'+Buffer.from(JSON.stringify({exp:Math.floor(Date.now()/1000)+3600,preferred_username:U})).toString('base64url')+'.y';
 const json=(r,body)=>r.fulfill({status:200,contentType:'application/json',body:JSON.stringify(body)});
 await ctx.route('**',r=>{
   const u=r.request().url();
   if(/action=whoami/.test(u))return json(r,{tier:'pro',tier_status:'active',role:'user'});
   if(/db-connect/.test(u)){
     let body={};try{body=JSON.parse(r.request().postData()||'{}');}catch(e){}
     CALLS.push(body.action||'?');
     if(body.action==='test')return json(r,{success:true,database:'SRC',user:'sa'});
     if(body.action==='schema-tables')return json(r,{success:true,database:'SRC',tables:TABLES,views:[]});
     if(body.action==='schema-fks')return json(r,{success:true,foreignKeys:FKS});
     if(body.action==='schema-columns'){
       const k=body.schemaName+'.'+body.tableName;
       return json(r,{success:true,table:COLS[k]||{schema:body.schemaName,name:body.tableName,columns:[],primaryKeys:[],uniques:[]}});
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
   localStorage.setItem('cygenix_cookie_consent',JSON.stringify({version:'1',essential:true,functional:true,timestamp:new Date().toISOString()}));
   const acct={homeAccountId:'h.t',environment:'cygenix.ciamlogin.com',tenantId:'t',username:a.U,localAccountId:'l',authorityType:'MSSTS',name:'You'};
   localStorage.setItem('acct-cygenix.ciamlogin.com-h.t',JSON.stringify(acct));
   localStorage.setItem('h.t-cygenix.ciamlogin.com-idtoken-f3478996-b2b5-4b21-9a23-a6b97a0e5b13-t-',
     JSON.stringify({credentialType:'IdToken',secret:a.tok,expiresOn:String(Math.floor(Date.now()/1000)+3600)}));
   const live={};live[a.U]={srcConnString:'mssql://u:p@src/SRC',srcConnMode:'direct'};
   localStorage.setItem('cygenix_project_connections',JSON.stringify(live));
 },{U,tok});

 const page=await ctx.newPage();
 const errs=[];page.on('pageerror',e=>errs.push(e.message));
 page.on('console',m=>{if(m.type()==='error'&&!/Failed to load resource|CygenixSync|favicon|fonts\./.test(m.text()))errs.push('console: '+m.text());});
 page.on('dialog',d=>d.accept());
 await page.goto('http://localhost:'+P+'/data-generator',{waitUntil:'domcontentloaded'});
 await page.waitForFunction(()=>!!window.CygenixDataGenModel&&typeof dgOpenPicker==='function',null,{timeout:20000});
 await page.waitForTimeout(500);

 check('the page boots with NO tables — the four demo tables are gone',
   await page.evaluate(()=>document.getElementById('dg-tables').textContent.indexOf('No tables chosen')>=0));
 check('and Generate is disabled, because this phase does not write',
   await page.evaluate(()=>document.getElementById('dg-generate-btn').disabled));

 await page.click('#dg-pick-btn');
 await page.waitForFunction(()=>document.querySelectorAll('#dg-pick-list .dg-pick-row').length>0,null,{timeout:15000});
 const picker=await page.evaluate(()=>({
   groups:Array.from(document.querySelectorAll('#dg-pick-grp,#dg-pick-list .dg-pick-grp')).map(g=>g.textContent),
   rows:Array.from(document.querySelectorAll('#dg-pick-list .dg-pick-row')).map(r=>r.textContent.trim()),
   db:document.getElementById('dg-pick-db').textContent}));
 check('the picker lists every user table, grouped by schema and not just dbo',
   picker.groups.join(',')==='dbo,fin'&&picker.rows.length===3,JSON.stringify(picker.groups));
 check('it shows the row count each table already has',
   /9,001 rows/.test(picker.rows.join(' '))&&/12 rows/.test(picker.rows.join(' ')),picker.rows.join(' | '));
 check('the whole database cost two calls, not one per table',
   CALLS.filter(c=>c==='schema-tables').length===1&&CALLS.filter(c=>c==='schema-fks').length===1,CALLS.join(','));

 await page.evaluate(()=>{const q=document.getElementById('dg-pick-q');q.value='matter';q.dispatchEvent(new Event('input'));});
 await page.waitForTimeout(150);
 check('search narrows it',
   await page.evaluate(()=>document.querySelectorAll('#dg-pick-list .dg-pick-row').length===1));

 await page.evaluate(()=>document.querySelector('#dg-pick-list .dg-pick-row input').click());
 await page.click('#dg-pick-go');
 await page.waitForFunction(()=>document.querySelectorAll('#dg-tables .dg-table-config').length>0,null,{timeout:15000});
 await page.waitForTimeout(300);

 const sel=await page.evaluate(()=>({
   names:Array.from(document.querySelectorAll('.dg-table-name')).map(n=>n.textContent.replace(/\s+/g,' ').trim()),
   order:document.getElementById('dg-order').textContent.replace(/\s+/g,' ').trim(),
   rows:(()=>{const o={};dgState().tables.forEach(t=>{o[t.key]=t.rows;});return o;})()}));
 check('picking a child pulls its parent in, and says the parent was added',
   sel.names.length===2&&sel.names.some(n=>/Client/.test(n)&&/parent/.test(n)),JSON.stringify(sel.names));
 check('the insert order is shown, parent first',
   /Insert order: 1\. Client → 2\. Matter/.test(sel.order),sel.order);
 check('the child gets 100 rows and the auto-added parent a fifth of that, floored at ten',
   sel.rows['dbo.matter']===100&&sel.rows['dbo.client']===20,JSON.stringify(sel.rows));
 check('the FK badge names the parent it points at',
   /FK → Client/.test(sel.names.join(' ')),sel.names.join(' | '));

 await page.evaluate(()=>dgToggleTable('dbo.client'));
 await page.waitForTimeout(200);
 const struct=await page.evaluate(()=>{
   const rows=Array.from(document.querySelectorAll('#dg-body-dbo\\.client .dg-cols tbody tr'));
   return rows.map(r=>Array.from(r.children).map(c=>c.textContent.trim()));});
 check('the structure is read-only — no text boxes to rename a real column',
   await page.evaluate(()=>!document.querySelector('#dg-body-dbo\\.client input[type=text]')));
 check('the identity and computed columns are shown and say why they are not written',
   struct.some(r=>r[1]==='Id'&&/identity/.test(r[4]))&&struct.some(r=>r[1]==='Display'&&/computed/.test(r[4])),JSON.stringify(struct));
 check('a unique column is marked as one',
   struct.some(r=>r[1]==='Code'&&/U/.test(r[0])));
 check('a generator can still be overridden per column',
   await page.evaluate(()=>!!document.querySelector('#dg-body-dbo\\.client select')));

 await page.evaluate(()=>{const t=Array.from(document.querySelectorAll('#dg-preview-tabs .dg-preview-tab'))
   .find(x=>/Matter/.test(x.textContent)); if(t) t.click();});
 await page.waitForTimeout(200);
 const prev=await page.evaluate(()=>document.getElementById('dg-preview-content').textContent);
 check('the preview shows the parent an FK would point at, not a bare number',
   /→ client\.Id/.test(prev),prev.slice(0,160));
 check('and it shows why an identity column has no value',
   /identity/.test(prev));

 check('NOTHING was written: every call was a read',
   CALLS.every(c=>['test','schema-tables','schema-fks','schema-columns'].indexOf(c)>=0),CALLS.join(','));

 const guard=await page.evaluate(async()=>{
   const before=document.getElementById('dg-log').textContent.length;
   await dgOpenPicker();
   const txt=document.getElementById('dg-log').textContent;
   return /wait a moment|Still reading/.test(txt)||txt.length===before;});
 check('a second read inside three seconds is refused rather than queued',guard);

 check('no page errors',errs.length===0,errs.join(' | '));
 await b.close();server.close();
 console.log('\n'+pass+' passed, '+fail+' failed');
 process.exit(fail?1:0);
})();
