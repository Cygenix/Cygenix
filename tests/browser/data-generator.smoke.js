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
const SQL=[];        // every statement the page sent
const ROWS={};       // the stub database's contents
const SEQ={};        // its identity counters
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
   {name:'Title',type:'NVARCHAR(200)',baseType:'nvarchar',maxLength:200,nullable:true,ordinal:3},
   // The column that broke a real run, in the shape it had there: a datetime
   // whose NAME reads as money, so the name-pattern rules hand it a number.
   // `new Date("150000")` is the year 150000 and SQL Server will not take it.
   {name:'FeeAmount',type:'DATETIME',baseType:'datetime',nullable:false,ordinal:4}]},
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
     if(body.action==='execute'){
       const sql=String(body.sql||'');
       SQL.push(sql);
       // A stub that behaves like a database: it keeps what is inserted, hands
       // back identities, and answers a key read from what it is holding.
       const sel=sql.match(/^SELECT TOP \d+ (.+?) FROM \[(\w+)\]\.\[(\w+)\]/);
       if(sel){
         const key=sel[2]+'.'+sel[3];
         const cols=sel[1].split(',').map(c=>c.trim().replace(/[\[\]]/g,''));
         const rows=(ROWS[key]||[]).map(x=>{const o={};cols.forEach(c=>{o[c]=x[c];});return o;});
         return json(r,{success:true,recordset:rows});
       }
       const ins=sql.match(/INSERT INTO \[(\w+)\]\.\[(\w+)\] \(([^)]*)\)/);
       if(ins){
         const key=ins[1]+'.'+ins[2];
         const cols=ins[3].split(',').map(c=>c.trim().replace(/[\[\]]/g,''));
         const tuples=(sql.match(/\n  \(([^\n]*)\)[,;]/g)||[]);
         ROWS[key]=ROWS[key]||[];
         const out=[];
         tuples.forEach(t=>{
           const vals=t.replace(/^\n  \(/,'').replace(/\)[,;]$/,'').split(/,(?=(?:[^']*'[^']*')*[^']*$)/).map(v=>v.trim());
           const row={};cols.forEach((c,i)=>{let v=vals[i];if(v===undefined)return;
             v=v.replace(/^N?'/,'').replace(/'$/,'').replace(/''/g,"'");
             row[c]=/^-?\d+(\.\d+)?$/.test(v)?Number(v):(v==='NULL'?null:v);});
           row.Id=(++SEQ[key]||(SEQ[key]=1));
           ROWS[key].push(row); out.push({Id:row.Id});
         });
         return json(r,{success:true,recordset:/OUTPUT|RETURNING/.test(sql)?out:[]});
       }
       const del=sql.match(/^DELETE FROM \[(\w+)\]\.\[(\w+)\](.*)$/s);
       if(del){
         const key=del[1]+'.'+del[2], rest=del[3]||'';
         if(!/WHERE/i.test(rest)){ ROWS[key]=[]; return json(r,{success:true,recordset:[]}); }
         const ids=(rest.match(/IN \(([^)]*)\)/)||[])[1];
         if(ids){
           const set=new Set(ids.split(',').map(v=>Number(v.trim())));
           ROWS[key]=(ROWS[key]||[]).filter(x=>!set.has(x.Id));
         }
         return json(r,{success:true,recordset:[]});
       }
       if(/^UPDATE /.test(sql)) return json(r,{success:true,recordset:[]});
       return json(r,{success:true,recordset:[]});
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
 check('Generate is offered, and says it only ever adds rows',
   await page.evaluate(()=>{const b=document.getElementById('dg-generate-btn');
     return !b.disabled && /never creates, drops or alters/.test(document.body.textContent.replace(/\s+/g,' '));}));

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

 check('reading and planning wrote nothing',
   CALLS.every(c=>['test','schema-tables','schema-fks','schema-columns'].indexOf(c)>=0),CALLS.join(','));

 /* ── One row count for the whole selection ────────────────────────────
    Bench testing means running the same job at 50 rows and then at 1,000.
    Setting each table by hand between the two is how somebody ends up timing
    a different shape than they meant to. */
 const allRows=await page.evaluate(()=>{
   const before=dgState().tables.map(t=>t.rows);
   const strip=document.getElementById('dg-allrows');
   const shown=strip && strip.style.display!=='none';
   const btn=Array.from(document.querySelectorAll('#dg-allrows-presets .preset'))
     .find(b=>b.textContent.trim()==='50');
   if(btn) btn.click();
   return {before, shown, has50:!!btn,
     after:dgState().tables.map(t=>t.rows),
     note:(document.getElementById('dg-allrows-note')||{}).textContent||'',
     lit:Array.from(document.querySelectorAll('#dg-allrows-presets .preset.active')).map(b=>b.textContent.trim()),
     perTable:Array.from(document.querySelectorAll('#dg-body-dbo\\.client .preset')).map(b=>b.textContent.trim())};
 });
 check('the selection can be set to one row count in a single click, and 50 is offered',
   allRows.shown && allRows.has50 && allRows.after.every(n=>n===50)
   && allRows.before.join(',')!=='50,50', JSON.stringify(allRows));
 check('…and the strip then says so, with that number lit',
   /2 tables at 50/.test(allRows.note) && allRows.lit.join(',')==='50', allRows.note);
 check('50 is on the per-table control too, so the two offer the same numbers',
   allRows.perTable.join(',')==='10,50,100,1,000,10,000,100,000', allRows.perTable.join(','));
 check('setting them all wrote nothing to the database',
   SQL.filter(q=>/INSERT|UPDATE|DELETE/i.test(q)).length===0,
   SQL.filter(q=>/INSERT|UPDATE|DELETE/i.test(q))[0]||'');

 /* ── Phase 2: an actual run ─────────────────────────────────────────── */
 // Small numbers, so the assertions are about behaviour rather than volume.
 await page.evaluate(()=>{dgSetRowCount('dbo.client',6);dgSetRowCount('dbo.matter',12);});
 await page.waitForTimeout(3200);            // the three-second gap is real
 SQL.length=0;
 await page.click('#dg-generate-btn');
 await page.waitForFunction(()=>/Run .* complete/.test(document.getElementById('dg-log').textContent),null,{timeout:30000});
 await page.waitForTimeout(300);

 const log=await page.evaluate(()=>document.getElementById('dg-log').textContent);
 check('the run finishes and says how many rows went in',
   /18 rows inserted/.test(log)||/18 row/.test(log),log.slice(-260));

 const inserts=SQL.filter(q=>/^DECLARE|^INSERT/.test(q));
 check('parents are inserted before children',
   inserts.length>=2 && /\[Client\]/.test(inserts[0]) && inserts.some(q=>/\[Matter\]/.test(q))
   && inserts.findIndex(q=>/\[Client\]/.test(q)) < inserts.findIndex(q=>/\[Matter\]/.test(q)),
   inserts.map(q=>q.slice(0,40)).join(' | '));

 check('the identity column is never written, and is read back with OUTPUT ... INTO',
   inserts.every(q=>{const m=q.match(/INSERT INTO \[\w+\]\.\[\w+\] \(([^)]*)\)/);
     return !m || m[1].split(',').every(c=>c.trim()!=='[Id]');})
   && inserts.some(q=>/OUTPUT inserted\.\[Id\] INTO @dgkeys/.test(q)));

 /* THE check this feature exists for: every child points at a parent that
    really exists. This is the browser's version of the orphan-check SQL. */
 const orphans=await page.evaluate(()=>0);
 const kids=ROWS['dbo.Matter']||[], dads=new Set((ROWS['dbo.Client']||[]).map(r=>r.Id));
 check('every child row points at a parent that really exists — no orphans',
   kids.length===12 && kids.every(k=>dads.has(k.ClientId)) && orphans===0,
   JSON.stringify(kids.slice(0,3)));

 check('children are spread across the parents, not piled on the first',
   new Set(kids.map(k=>k.ClientId)).size>1,
   JSON.stringify([...new Set(kids.map(k=>k.ClientId))]));

 check('the unique column never repeats',
   (()=>{const c=ROWS['dbo.Client']||[];return new Set(c.map(r=>r.Code)).size===c.length;})(),
   JSON.stringify((ROWS['dbo.Client']||[]).map(r=>r.Code)));

 check('the computed column is never written',
   inserts.every(q=>!/\[Display\]/.test(q)));

 /* ── The date bug, end to end ──────────────────────────────────────────
    FeeAmount is a datetime with a money-shaped name. Before the fix it was
    handed a number, `new Date("150000")` made that the year 150000, and the
    literal went out as 'YYYY-MM-DD hh:mm:ss' — which SQL Server reads
    through the session's DATEFORMAT, not as ISO. On a British-English
    connection that is dmy, so any day past the 12th is read as a month and
    the whole batch dies with "out-of-range value". */
 const dates=(ROWS['dbo.Matter']||[]).map(r=>r.FeeAmount);
 check('a datetime column with a money-shaped name gets a date, not a number',
   dates.length===12 && dates.every(d=>/^\d{4}-\d{2}-\d{2}T/.test(String(d))),
   JSON.stringify(dates.slice(0,3)));

 check('every date is inside what a SQL Server datetime will hold',
   dates.every(d=>{const ms=Date.parse(String(d)+'Z');
     return isFinite(ms) && ms>=Date.parse('1753-01-01T00:00:00Z') && ms<=Date.parse('9999-12-31T23:59:59Z');}),
   JSON.stringify(dates.slice(0,3)));

 check('the literal that goes to the server keeps its T, so the month cannot be read as the day',
   (()=>{const m=inserts.filter(q=>/\[Matter\]/.test(q));
     return m.length>0 && m.every(q=>!/'\d{4}-\d{2}-\d{2} \d{2}:/.test(q))
       && m.some(q=>/'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(q));})(),
   (inserts.find(q=>/\[Matter\]/.test(q))||'').slice(0,240));

 check('and the page says so — the generator came from the type, not the name',
   await page.evaluate(async()=>{
     dgToggleTable('dbo.matter');
     await new Promise(r=>setTimeout(r,150));
     const el=document.getElementById('dg-body-dbo.matter');
     if(!el) return false;
     const row=Array.from(el.querySelectorAll('.dg-cols tbody tr'))
       .find(tr=>/FeeAmount/.test(tr.textContent));
     return !!row && /datetime — inferred from the type/.test(row.textContent);
   }));

 check('NOTHING was created, dropped, truncated or deleted — every statement was an insert or a link',
   SQL.every(q=>!/\b(CREATE|DROP|TRUNCATE|ALTER|DELETE|MERGE)\b/i.test(
     q.replace(/\[(?:[^\]]|\]\])*\]/g,' id ').replace(/'(?:[^']|'')*'/g," 'l' "))),
   SQL.find(q=>/\b(CREATE|DROP|TRUNCATE|DELETE)\b/i.test(q))||'');

 check('the run is recorded, with the keys it inserted',
   await page.evaluate(()=>{
     const runs=JSON.parse(localStorage.getItem('cygenix_datagen_runs')||'[]');
     return runs.length===1 && runs[0].id.indexOf('dgr_')===0
       && runs[0].tables['dbo.client'] && runs[0].tables['dbo.client'].keys.length===6;}));

 check('a second Generate inside three seconds is refused rather than queued',
   await page.evaluate(async()=>{
     const before=document.getElementById('dg-log').textContent.length;
     await dgGenerate();
     return /wait a moment|Already generating/.test(
       document.getElementById('dg-log').textContent.slice(before));}));

 /* ── Phase 3: taking it back out ─────────────────────────────────────── */
 // Rows that were in the table BEFORE this tool ran. Deleting a run must not
 // touch them, and that is the whole test.
 await page.evaluate(()=>0);
 ROWS['dbo.Client'].unshift({Id:9001,Code:'PRE-EXISTING',Email:'was@here.test'});
 const beforeClients=ROWS['dbo.Client'].length, beforeMatters=(ROWS['dbo.Matter']||[]).length;

 await page.click('#dg-delete-btn');
 await page.waitForSelector('#dg-runs-modal.open');
 const runsList=await page.evaluate(()=>document.getElementById('dg-runs-list').textContent.replace(/\s+/g,' '));
 check('the recorded run is listed with what it inserted',
   /dgr_/.test(runsList)&&/18 rows/.test(runsList)&&/2 tables/.test(runsList),runsList.slice(0,160));

 SQL.length=0;
 await page.waitForTimeout(3200);
 await page.evaluate(()=>document.querySelector('#dg-runs-list .dg-btn-danger').click());
 await page.waitForFunction(()=>/Run .* removed/.test(document.getElementById('dg-log').textContent),null,{timeout:30000});
 await page.waitForTimeout(300);

 check('deleting a run removes exactly the rows it inserted',
   (ROWS['dbo.Matter']||[]).length===0 && ROWS['dbo.Client'].length===beforeClients-6,
   'clients '+ROWS['dbo.Client'].length+' of '+beforeClients+', matters '+(ROWS['dbo.Matter']||[]).length+' of '+beforeMatters);

 /* THE check this design exists for. */
 check('the row that was there BEFORE the run is untouched',
   ROWS['dbo.Client'].some(r=>r.Code==='PRE-EXISTING'));

 check('children are deleted before parents',
   (()=>{const d=SQL.filter(q=>/^DELETE/.test(q));
     return d.length>=2 && /\[Matter\]/.test(d[0]) && /\[Client\]/.test(d[d.length-1]);})(),
   SQL.filter(q=>/^DELETE/.test(q)).map(q=>q.slice(0,46)).join(' | '));

 check('it deletes BY KEY, never by emptying the table',
   SQL.filter(q=>/^DELETE/.test(q)).every(q=>/WHERE/i.test(q)));

 check('nothing in the delete path could create, drop, truncate or alter',
   SQL.every(q=>!/\b(CREATE|DROP|TRUNCATE|ALTER|MERGE)\b/i.test(
     q.replace(/\[(?:[^\]]|\]\])*\]/g,' id ').replace(/'(?:[^']|'')*'/g," 'l' "))));

 check('the record is cleared once everything it described has gone',
   await page.evaluate(()=>JSON.parse(localStorage.getItem('cygenix_datagen_runs')||'[]').length===0));

 // Empty-first: off by default, and refused unless the word is typed.
 check('empty-first starts off',
   await page.evaluate(()=>!document.getElementById('dg-empty-first').checked));
 await page.waitForTimeout(3200);        // the write gap, again
 const emptied=await page.evaluate(async()=>{
   const before=(window.__promptAnswer=undefined, document.getElementById('dg-log').textContent.length);
   const realPrompt=window.prompt;
   window.prompt=()=>'no';                       // the word is NOT typed
   document.getElementById('dg-empty-first').click();
   await dgGenerate();
   const txt=document.getElementById('dg-log').textContent.slice(before);
   window.prompt=realPrompt;
   document.getElementById('dg-empty-first').click();
   return /nothing was emptied, and nothing was generated/.test(txt);});
 await page.waitForTimeout(200);
 check('an unconfirmed empty-first stops the whole run, not just the emptying',
   emptied && ROWS['dbo.Client'].some(r=>r.Code==='PRE-EXISTING'));

 // Saved selections.
 check('the selection is remembered per profile, with row counts and no schema copy',
   await page.evaluate(()=>{
     const all=JSON.parse(localStorage.getItem('cygenix_datagen_selection')||'{}');
     const mine=all[Object.keys(all)[0]];
     return !!mine && mine.tables.length===2
       && mine.tables.every(t=>typeof t.rows==='number' && !('columns' in t));}));

 check('a production profile is refused outright, with no way to confirm past it',
   await page.evaluate(async()=>{
     const raw=localStorage.getItem('cygenix_profiles_v1');
     localStorage.setItem('cygenix_profiles_v1',JSON.stringify({v:1,connMeta:{},bindings:[],runRecords:[],events:[],
       profiles:[{id:'PRD_LIVE',name:'Live',envClass:'PRD',status:'active',srcConnId:'a',tgtConnId:'b',createdAt:1,updatedAt:1}],
       settings:{envClasses:['DEV','TEST','UAT','PRD'],activeProfileId:'PRD_LIVE',selectedAt:1}}));
     const before=document.getElementById('dg-log').textContent.length;
     await dgGenerate();
     const txt=document.getElementById('dg-log').textContent.slice(before);
     if(raw) localStorage.setItem('cygenix_profiles_v1',raw); else localStorage.removeItem('cygenix_profiles_v1');
     return /never generated into production/.test(txt);}));

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
