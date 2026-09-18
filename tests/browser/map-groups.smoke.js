/* tests/browser/map-groups.smoke.js
 * ---------------------------------------------------------------------------
 * Object Mapping's map groups, in a real browser.
 *
 * tests/map-groups.test.js proves the rules and the wiring. This walks the
 * things only a browser can answer: that the four starter groups are seeded
 * and PERSISTED on first load, that an ungrouped card is untouched, that a
 * grouped one gets a 4px stripe and a named pill, that a map whose group was
 * deleted shows as ungrouped without being rewritten, that a recolour repaints
 * every card in that group with no reload, and that the Browse-all chips
 * filter what they say they filter.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/map-groups.smoke.js
 */
'use strict';
const http=require('http'),fs=require('fs'),path=require('path');
const {chromium}=require('playwright-core');
const PUB=path.join(__dirname,'..','..','public');
const P=8477, EXE='/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
let pass=0,fail=0;
const check=(l,ok,x)=>{ok?(pass++,console.log('  PASS  '+l)):(fail++,console.log('  FAIL  '+l+(x?'  → '+String(x).slice(0,300):'')));};
const TYPES={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.svg':'image/svg+xml'};
const ROUTES={};fs.readFileSync(path.join(PUB,'_redirects'),'utf8').split('\n').forEach(l=>{const m=l.trim().match(/^(\/\S*)\s+(\/\S+)\s+200$/);if(m)ROUTES[m[1]]=m[2];});
const server=http.createServer((rq,rs)=>{let p=decodeURIComponent(rq.url.split('?')[0]);if(p==='/')p='/index.html';if(ROUTES[p])p=ROUTES[p];let f=path.join(PUB,p);if(!fs.existsSync(f)&&fs.existsSync(f+'.html'))f+='.html';if(!f.startsWith(PUB)||!fs.existsSync(f)||fs.statSync(f).isDirectory()){rs.writeHead(404);return rs.end('no');}rs.writeHead(200,{'Content-Type':TYPES[path.extname(f)]||'application/octet-stream'});rs.end(fs.readFileSync(f));});
const U='you@example.test';
(async()=>{
  await new Promise(r=>server.listen(P,r));
  const b=await chromium.launch({executablePath:EXE,args:['--no-sandbox']});
  const ctx=await b.newContext({viewport:{width:1500,height:950}});
  const tok='x.'+Buffer.from(JSON.stringify({exp:Math.floor(Date.now()/1000)+3600,preferred_username:U})).toString('base64url')+'.y';
  await ctx.route('**',r=>{const u=r.request().url();
    if(/action=whoami/.test(u))return r.fulfill({status:200,contentType:'application/json',body:JSON.stringify({tier:'pro',tier_status:'active',role:'user'})});
    if(u.startsWith('http://localhost:'+P))return r.continue();
    return r.fulfill({status:200,contentType:'application/json',body:'{}'});});
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
    localStorage.setItem('cygenix_projects',JSON.stringify([{id:'p1',name:'Acme'}]));
    localStorage.setItem('cygenix_active_project_id','p1');
    localStorage.setItem('cygenix_jobs',JSON.stringify([
      {id:'j1',name:'Customers',jobType:'simple-map',projectId:'p1',source:'dbo.CUST',target:'dbo.Client',columnMapping:[{srcCol:'a',tgtCol:'b'}],created:new Date().toISOString()},
      {id:'j2',name:'Invoices',jobType:'simple-map',projectId:'p1',source:'dbo.INV',target:'dbo.Vchr',columnMapping:[{srcCol:'a',tgtCol:'b'}],created:new Date().toISOString(),groupId:'grp_txn'},
      {id:'j3',name:'Orphan',jobType:'simple-map',projectId:'p1',source:'dbo.X',target:'dbo.Y',columnMapping:[],created:new Date().toISOString(),groupId:'grp_deleted'}
    ]));
  },{U,tok});
  const page=await ctx.newPage();
  const errs=[];page.on('pageerror',e=>errs.push(e.message));
  // Only real page errors. Console 404s for fonts and favicons, and the sync
  // layer complaining about the stubbed endpoints, are this harness talking to
  // itself — counting them would make the check fail for reasons that have
  // nothing to do with the feature.
  page.on('console',m=>{
    if (m.type()!=='error') return;
    const t=m.text();
    if (/Failed to load resource|CygenixSync|favicon|fonts\./.test(t)) return;
    errs.push('console: '+t);
  });
  page.on('dialog',d=>d.accept());
  await page.goto('http://localhost:'+P+'/object-mapping',{waitUntil:'domcontentloaded'});
  await page.waitForFunction(()=>!!window.CygenixMapGroups&&typeof mgStore==='function',null,{timeout:20000});
  await page.waitForTimeout(900);

  const seeded=await page.evaluate(()=>({stored:JSON.parse(localStorage.getItem('cygenix_map_groups')||'null'),names:mgStore().groups.map(g=>g.name)}));
  check('four starter groups seeded and persisted on first load',
    seeded.stored&&seeded.stored.groups.length===4&&seeded.names.join(',')==='Master data,Transactional,Reference,Configuration',JSON.stringify(seeded.names));

  const cards=await page.evaluate(()=>Array.from(document.querySelectorAll('#rm-grid .rm-card')).map(c=>({
    name:(c.querySelector('.rm-name')||{}).textContent,has:c.classList.contains('mg-has'),
    pill:(c.querySelector('.mg-pill-name')||{}).textContent||'',stripe:getComputedStyle(c).borderLeftWidth})));
  check('an ungrouped map looks exactly as before — no stripe, no pill',
    cards.some(c=>/Customers/.test(c.name)&&!c.has&&!c.pill),JSON.stringify(cards));
  check('a grouped map gets a 4px stripe and a NAMED pill',
    cards.some(c=>/Invoices/.test(c.name)&&c.has&&c.pill==='Transactional'&&c.stripe==='4px'),JSON.stringify(cards));
  check('a map whose group was deleted shows as ungrouped, and is not rewritten',
    cards.some(c=>/Orphan/.test(c.name)&&!c.has)
    && (await page.evaluate(()=>_getAllSavedJobs().find(j=>j.id==='j3').groupId))==='grp_deleted');

  const legend=await page.evaluate(()=>({txt:($('mg-legend')||{}).textContent||'',shown:($('mg-legend')||{}).style.display}));
  check('the legend names only the groups on screen, plus Ungrouped',
    legend.shown!=='none'&&/Transactional/.test(legend.txt)&&/Ungrouped/.test(legend.txt)&&!/Reference/.test(legend.txt),JSON.stringify(legend));

  check('the dropdown is disabled with no map open, and says why',
    await page.evaluate(()=>$('mg-btn').disabled&&/Choose source and target/.test($('mg-btn').title)));

  // Recolour and watch every card follow, with no reload.
  await page.evaluate(()=>{const r=CygenixMapGroups.mgUpdate(mgStore(),'grp_txn',{color:'#8E5BC7'});mgWriteStore(r.store);});
  await page.waitForTimeout(250);
  check('recolouring a group repaints its cards straight away',
    await page.evaluate(()=>{const c=Array.from(document.querySelectorAll('.rm-card')).find(x=>/Invoices/.test(x.textContent));
      return c&&c.classList.contains('mg-has')&&/142|8E5BC7|rgb\(142/.test(getComputedStyle(c).borderLeftColor);}));

  const dup=await page.evaluate(()=>CygenixMapGroups.mgAdd(mgStore(),{name:'master data',color:'#3A9E5F'}));
  check('a duplicate name is refused in the live page too',dup.ok===false);

  // Browse-all chips
  await page.evaluate(()=>openLoadMapModal());
  await page.waitForTimeout(350);
  const chips=await page.evaluate(()=>Array.from(document.querySelectorAll('#mg-chips .mg-chip')).map(c=>c.textContent.trim()));
  check('Browse all shows All, the groups in use, and Ungrouped',
    chips.length===3&&/^All/.test(chips[0])&&/Transactional/.test(chips[1])&&/Ungrouped/.test(chips[2]),JSON.stringify(chips));
  await page.evaluate(()=>mgSetFilter('grp_txn'));
  await page.waitForTimeout(250);
  check('picking a chip filters the list to that group',
    await page.evaluate(()=>{const t=$('load-map-list').textContent;return /Invoices/.test(t)&&!/Customers/.test(t);}));
  await page.evaluate(()=>mgSetFilter('__none__'));
  await page.waitForTimeout(250);
  check('Ungrouped shows the ungrouped maps, dangling group included',
    await page.evaluate(()=>{const t=$('load-map-list').textContent;return /Customers/.test(t)&&/Orphan/.test(t)&&!/Invoices/.test(t);}));

  check('no page errors',errs.length===0,errs.join(' | '));
  await b.close();server.close();
  console.log('\n'+pass+' passed, '+fail+' failed');
  process.exit(fail?1:0);
})();
