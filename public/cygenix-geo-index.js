/* ============================================================================
   cygenix-geo-index.js — where the estate's data sits, geographically
   ----------------------------------------------------------------------------
   The discovery and aggregation half of the Globe view. No DOM, no fetch, no
   storage: it is handed a schema and an execute function and hands back a
   ledger. The globe draws that ledger; this file decides what is in it.

   Three steps, each usable on its own:

     geoIndex(tables, edges, opts)   which tables carry geography, and how
     geoPlan(index, opts)            one GROUP BY per geo-bearing table
     geoRun(index, opts)             execute the plan and fold the results

   Detection is a heuristic over column names plus foreign keys into the
   estate's own Country table. On a 224-family estate it will get some wrong,
   which is why every entry carries the evidence that put it there and can be
   overridden — a silently wrong map is worse than an editable one.

   Nothing here plots an address. Every query is a GROUP BY with a COUNT, so
   the finest thing that can come back is "how many records in this region".
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (typeof window !== 'undefined') window.CygenixGeoIndex = api;
})(this, function () {
'use strict';

/* ---------- column name patterns ----------------------------------------
   Matched on WORDS, not substrings. A column name is split at camel-case and
   punctuation boundaries first, so BillToCountryCode is [bill, to, country,
   code] and AddressCountry is [address, country] — and, just as importantly,
   RealEstate is [real, estate] rather than a name containing "state".

   Roles are the raw ones. Which of county and city is admin-1 and which is
   admin-2 depends on what else the table carries, and is settled per table. */
var ROLE_WORDS = {
  country: ['country', 'countries', 'nationality', 'jurisdiction', 'domicile',
            'iso2', 'iso3', 'isocode', 'countrycode'],
  state:   ['state', 'province', 'stateprovince', 'region'],
  county:  ['county', 'shire'],
  city:    ['city', 'town', 'locality', 'municipality'],
  postal:  ['postcode', 'postalcode', 'zip', 'zipcode', 'postzip', 'pcode', 'eircode'],
  lat:     ['lat', 'latitude', 'geolat'],
  lng:     ['long', 'lng', 'lon', 'longitude', 'geolong'],
};
/* Two words that only mean something together: post + code, zip + code. */
var ROLE_PAIRS = [
  ['postal', ['post', 'postal', 'zip'], ['code', 'cd']],
  ['country', ['country'], ['code', 'cd', 'id', 'key', 'iso', 'name']],
];
/* A word that turns a name into an identifier rather than a value. */
var ID_TAIL = /^(id|key|index|no|num|ref|fk|sk)$/;

/* Shadow copies of real tables. They duplicate the geography of the table
   they shadow, so counting them would double the estate. Excluded by
   default; the caller can put them back. */
var SHADOW = /(_|\b)(draft|template|audit|shadow|bak|backup|archive|temp|tmp|test|copy|old)(\d*)$/i;

function lower(s){ return String(s == null ? '' : s).toLowerCase(); }
function bare(s){ return lower(s).replace(/[^a-z0-9]/g, ''); }

function words(name){
  return String(name == null ? '' : name)
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
    .toLowerCase().split(/[^a-z0-9]+/).filter(Boolean);
}
var ROLE_ORDER = ['country', 'state', 'county', 'city', 'postal', 'lat', 'lng'];

/* Which geo role, if any, does this column carry? */
function geoColumnRole(name){
  var w = words(name);
  if (!w.length) return null;
  for (var i = 0; i < ROLE_PAIRS.length; i++) {
    var role = ROLE_PAIRS[i][0], heads = ROLE_PAIRS[i][1], tails = ROLE_PAIRS[i][2];
    for (var k = 0; k < w.length - 1; k++) {
      if (heads.indexOf(w[k]) >= 0 && tails.indexOf(w[k + 1]) >= 0) return role;
    }
  }
  for (var r = 0; r < ROLE_ORDER.length; r++) {
    var list = ROLE_WORDS[ROLE_ORDER[r]];
    for (var j = 0; j < w.length; j++) if (list.indexOf(w[j]) >= 0) return ROLE_ORDER[r];
  }
  return null;
}
/* CountryId names a row in the reference table; Country names a place. */
function isIdColumn(name){
  var w = words(name);
  return w.length > 1 && ID_TAIL.test(w[w.length - 1]);
}
function isShadowTable(name){ return SHADOW.test(String(name || '')); }

/* Is this the estate's ISO reference table? The brief's estate has one —
   Country, 247 rows — and any table with a key into it is geo-bearing even
   with no country column of its own. */
function isCountryRef(name){ return /^(country|countries|countrylist|refcountry|isocountry)$/.test(bare(name)); }

/* ---------- the index ----------------------------------------------------
   tables: smTables() records — { name, key, columns:[{name}|'name'], kind }
   edges:  the graph's declared foreign keys — { from, to } table keys        */
function geoIndex(tables, edges, opts){
  opts = opts || {};
  var includeShadow = !!opts.includeShadow;
  var classify = opts.classify || function () { return 7; };
  var familyKey = opts.familyKey || function (n) { return n; };

  var byKey = {}, countryRefKeys = {};
  (tables || []).forEach(function (t) {
    if (t.key) byKey[t.key] = t;
    if (isCountryRef(t.name) && t.key) countryRefKeys[t.key] = t.name;
  });

  /* Which tables point at the country reference table, and through which
     column — the join the aggregation has to make. */
  var fkToCountry = {};
  (edges || []).forEach(function (e) {
    if (!countryRefKeys[e.to] || e.from === e.to) return;
    fkToCountry[e.from] = { table: countryRefKeys[e.to], key: e.to, column: e.fromColumn || null,
                            refColumn: e.toColumn || null };
  });

  var out = [];
  (tables || []).forEach(function (t) {
    /* The reference table is the dictionary, not the data. Counting its rows
       would put one record in every country it lists. */
    if (isCountryRef(t.name)) return;
    var shadow = isShadowTable(t.name);
    if (shadow && !includeShadow) return;

    var found = {};
    var names = (t.columns || []).map(function (c) { return typeof c === 'string' ? c : (c && c.name); })
                                 .filter(Boolean);
    names.forEach(function (n) {
      var role = geoColumnRole(n);
      if (!role) return;
      /* A plain name beats an id for the same role: given Country and
         CountryId, group by the one that holds a place. */
      if (!found[role] || (isIdColumn(found[role]) && !isIdColumn(n))) found[role] = n;
    });

    /* A US county sits inside a state; a UK county is the top division. So
       county is admin-2 when the table also names a state, and admin-1 when
       it does not — and city is the finer level only when county is not
       already filling it. */
    var admin1 = found.state || found.county || null;
    var admin2 = found.state ? (found.county || found.city || null)
                             : (found.county ? found.city || null : found.city || null);
    if (admin2 && admin1 === admin2) admin2 = null;

    var fk = fkToCountry[t.key];
    /* Grouping by CountryId returns row ids, which are not places. When the
       key into the reference table is known, join it and take the code. */
    var countryCol = found.country && !isIdColumn(found.country) ? found.country : null;
    var useFk = !countryCol && !!fk;
    if (!countryCol && !fk && found.country) countryCol = found.country;

    var hasAny = countryCol || admin1 || admin2 || found.postal || (found.lat && found.lng);
    if (!hasAny && !fk) return;

    out.push({
      table: t.name,
      key: t.key || t.name,
      rows: t.rowCount || 0,
      /* What "one entity" means in this table, for the distinct metric. */
      entityKey: (t.primaryKeys && t.primaryKeys.length === 1) ? t.primaryKeys[0] : null,
      family: familyKey(t.name),
      domain: classify(t.name),
      /* 0 world, 1 admin-1, 2 admin-2: the finest the table can answer. */
      level: admin2 ? 2 : (admin1 ? 1 : 0),
      /* state and county here mean admin-1 and admin-2, whichever columns
         those turned out to be. */
      columns: { country: countryCol, state: admin1, county: admin2,
                 postcode: found.postal || null, lat: found.lat || null, lng: found.lng || null },
      via: useFk ? 'fk:' + fk.table : 'direct',
      fk: useFk ? fk : null,
      shadow: shadow,
      enabled: true,
    });
  });

  /* Heaviest first: the tables that decide the shape of the map are the ones
     worth looking at, and worth querying first. */
  out.sort(function (a, b) { return b.rows - a.rows; });
  return out;
}

/* ---------- the queries --------------------------------------------------
   One GROUP BY per table. Identifiers are bracket-quoted, and any bracket
   inside a name is doubled — these come from the catalogue rather than from
   a user, but a name is still not a keyword. */
function q(id){ return '[' + String(id).replace(/]/g, ']]') + ']'; }
function qualify(key, table){
  var parts = String(key || table).split('.');
  if (parts.length >= 2) return q(parts[0]) + '.' + q(parts.slice(1).join('.'));
  return q(table);
}

function geoSql(entry, opts){
  opts = opts || {};
  var top = opts.top || 5000;
  var c = entry.columns, src = qualify(entry.key, entry.table);
  var sel = [], grp = [];
  var alias = 't';
  function add(col, as, owner){
    sel.push((owner || alias) + '.' + q(col) + ' AS ' + q(as));
    grp.push((owner || alias) + '.' + q(col));
  }

  var join = '';
  if (!c.country && entry.fk && entry.fk.column) {
    /* The country is an id into the reference table, so the code has to come
       back from there — an id means nothing on a map. */
    var refName = entry.fk.table;
    join = ' LEFT JOIN ' + qualify(entry.fk.key, refName) + ' c ON t.' + q(entry.fk.column)
         + ' = c.' + q(entry.fk.refColumn || 'Id');
    sel.push('c.' + q(opts.refCodeColumn || 'Code') + ' AS ' + q('country'));
    grp.push('c.' + q(opts.refCodeColumn || 'Code'));
  } else if (c.country) add(c.country, 'country');

  if (c.state)    add(c.state, 'state');
  if (c.county)   add(c.county, 'county');
  if (c.postcode) add(c.postcode, 'postcode');
  if (!sel.length) return null;

  /* Row count over-weights the tables that hold many rows per entity — an
     address book beats a client list. Counting the entity key instead asks
     "how many clients", which is the question most people mean. Only offered
     where the table has a key to count. */
  var count = opts.metric === 'distinct' && entry.entityKey
    ? 'COUNT(DISTINCT t.' + q(entry.entityKey) + ')'
    : 'COUNT(*)';

  return 'SELECT TOP ' + top + ' ' + sel.join(', ') + ', ' + count + ' AS ' + q('n')
       + ' FROM ' + src + ' t' + join
       + ' GROUP BY ' + grp.join(', ')
       + ' ORDER BY COUNT(*) DESC';
}

function geoPlan(index, opts){
  var plan = [];
  (index || []).forEach(function (e) {
    if (e.enabled === false) return;
    var sql = geoSql(e, opts);
    if (sql) plan.push({ entry: e, table: e.table, key: e.key, sql: sql });
  });
  return plan;
}

/* ---------- ISO-3166-1 -------------------------------------------------- */
/* Natural Earth's admin-0 names, alternates and formal names, normalised to
   letters and digits. Bundled rather than fetched: normalisation runs before
   anything is drawn, and a map that cannot name a country until a file
   arrives is a map that renders wrong first. Format, one per country:
     ISO3 ISO2 Display_Name alias|alias|alias                                */
var ISO_RAW =
  'IDN ID Indonesia indonesia|republicofindonesia;MYS MY Malaysia malaysia;CHL CL Chile chile|r' +
  'epublicofchile;BOL BO Bolivia bolivia|plurinationalstateofbolivia;PER PE Peru peru|republico' +
  'fperu;ARG AR Argentina argentina|argentinerepublic;ESB -- Dhekelia dhekelia|dhekeliasovereig' +
  'nbasearea;CYP CY Cyprus cyprus|republicofcyprus;IND IN India india|republicofindia;CHN CN Ch' +
  'ina china|peoplesrepublicofchina;ISR IL Israel israel|stateofisrael;PSE PS Palestine palesti' +
  'ne|palestinewestbankandgaza|westbankandgaza;LBN LB Lebanon lebaneserepublic|lebanon;ETH ET E' +
  'thiopia ethiopia|federaldemocraticrepublicofethiopia;SSD SS S._Sudan republicofsouthsudan|so' +
  'uthsudan|ssudan;SOM SO Somalia federalrepublicofsomalia|somalia;KEN KE Kenya kenya|republico' +
  'fkenya;MWI MW Malawi malawi|republicofmalawi;TZA TZ Tanzania tanzania|unitedrepublicoftanzan' +
  'ia;SYR SY Syria syria|syrianarabrepublic;SOL -- Somaliland republicofsomaliland|somaliland;F' +
  'RA FR France clippertoni|clippertonisland|france|frenchrepublic;SUR SR Suriname republicofsu' +
  'riname|suriname;GUY GY Guyana cooperativerepublicofguyana|guyana;KOR KR South_Korea korearep' +
  '|koreasouth|republicofkorea|southkorea;PRK KP North_Korea democraticpeoplesrepublicofkorea|d' +
  'emrepkorea|koreademrep|koreanorth|northkorea;MAR MA Morocco kingdomofmorocco|morocco;ESH EH ' +
  'W._Sahara sahrawiarabdemocraticrepublic|westernsahara|wsahara;CRI CR Costa_Rica costarica|re' +
  'publicofcostarica;NIC NI Nicaragua nicaragua|republicofnicaragua;COG CG Congo congo|congorep' +
  '|congorepublicofthe|republicofthecongo;COD CD Dem._Rep._Congo congodemocraticrepublicofthe|c' +
  'ongodemrep|democraticrepublicofthecongo|demrepcongo;BTN BT Bhutan bhutan|kingdomofbhutan;UKR' +
  ' UA Ukraine ukraine;BLR BY Belarus belarus|republicofbelarus;NAM NA Namibia namibia|republic' +
  'ofnamibia;ZAF ZA South_Africa republicofsouthafrica|southafrica;MAF MF St-Martin saintmartin' +
  '|saintmartinfrenchpart|stmartin|stmartinfrenchpart;SXM SX Sint_Maarten sintmaarten|sintmaart' +
  'endutchpart|stmaartendutchpart;OMN OM Oman oman|sultanateofoman;UZB UZ Uzbekistan republicof' +
  'uzbekistan|uzbekistan;KAZ KZ Kazakhstan baikonur|baikonurcosmodrome|baykonur|baykonurcosmodr' +
  'ome|kazakhstan|republicofkazakhstan;TJK TJ Tajikistan republicoftajikistan|tajikistan;LTU LT' +
  ' Lithuania lithuania|republicoflithuania;BRA BR Brazil brazil|brazilbraziliani|braziliani|br' +
  'azilianisland|federativerepublicofbrazil;URY UY Uruguay orientalrepublicofuruguay|uruguay;MN' +
  'G MN Mongolia mongolia;RUS RU Russia russia|russianfederation;CZE CZ Czechia cesko|czechia|c' +
  'zechrepublic;DEU DE Germany federalrepublicofgermany|germany;EST EE Estonia estonia|republic' +
  'ofestonia;LVA LV Latvia latvia|republicoflatvia;NOR NO Norway kingdomofnorway|norway;SWE SE ' +
  'Sweden kingdomofsweden|sweden;FIN FI Finland finland|republicoffinland;VNM VN Vietnam social' +
  'istrepublicofvietnam|vietnam;KHM KH Cambodia cambodia|kingdomofcambodia;LUX LU Luxembourg gr' +
  'andduchyofluxembourg|luxembourg;ARE AE United_Arab_Emirates unitedarabemirates;BEL BE Belgiu' +
  'm belgium|kingdomofbelgium;GEO GE Georgia georgia;MKD MK North_Macedonia northmacedonia|repu' +
  'blicofnorthmacedonia;ALB AL Albania albania|republicofalbania;AZE AZ Azerbaijan azerbaijan|r' +
  'epublicofazerbaijan;KOS XK Kosovo kosovo|republicofkosovo;TUR TR Turkey republicofturkey|tur' +
  'key;ESP ES Spain kingdomofspain|spain;LAO LA Laos laopdr|laopeoplesdemocraticrepublic|laos;K' +
  'GZ KG Kyrgyzstan kyrgyzrepublic|kyrgyzstan;ARM AM Armenia armenia|republicofarmenia;DNK DK D' +
  'enmark denmark|kingdomofdenmark;LBY LY Libya libya;TUN TN Tunisia republicoftunisia|tunisia;' +
  'ROU RO Romania romania;HUN HU Hungary hungary|republicofhungary;SVK SK Slovakia slovakia|slo' +
  'vakrepublic;POL PL Poland poland|republicofpoland;IRL IE Ireland ireland;GBR GB United_Kingd' +
  'om unitedkingdom|unitedkingdomofgreatbritainandnorthernireland;GRC GR Greece greece|hellenic' +
  'republic;ZMB ZM Zambia republicofzambia|zambia;SLE SL Sierra_Leone republicofsierraleone|sie' +
  'rraleone;GIN GN Guinea guinea|republicofguinea;LBR LR Liberia liberia|republicofliberia;CAF ' +
  'CF Central_African_Rep. centralafricanrep|centralafricanrepublic;SDN SD Sudan republicofthes' +
  'udan|sudan;DJI DJ Djibouti djibouti|republicofdjibouti;ERI ER Eritrea eritrea|stateoferitrea' +
  ';AUT AT Austria austria|republicofaustria;IRQ IQ Iraq iraq|republicofiraq;ITA IT Italy itali' +
  'anrepublic|italy;CHE CH Switzerland swissconfederation|switzerland;IRN IR Iran iran|iranisla' +
  'micrep|islamicrepublicofiran;NLD NL Netherlands kingdomofthenetherlands|netherlands;LIE LI L' +
  'iechtenstein liechtenstein|principalityofliechtenstein;CIV CI Côte_d\'Ivoire cotedivoire|ivor' +
  'ycoast|republicofivorycoast;SRB RS Serbia republicofserbia|serbia;MLI ML Mali mali|republico' +
  'fmali;SEN SN Senegal republicofsenegal|senegal;NGA NG Nigeria federalrepublicofnigeria|niger' +
  'ia;BEN BJ Benin benin|republicofbenin;AGO AO Angola angola|peoplesrepublicofangola;HRV HR Cr' +
  'oatia croatia|republicofcroatia;SVN SI Slovenia republicofslovenia|slovenia;QAT QA Qatar qat' +
  'ar|stateofqatar;SAU SA Saudi_Arabia kingdomofsaudiarabia|saudiarabia;BWA BW Botswana botswan' +
  'a|republicofbotswana;ZWE ZW Zimbabwe republicofzimbabwe|zimbabwe;PAK PK Pakistan islamicrepu' +
  'blicofpakistan|pakistan;BGR BG Bulgaria bulgaria|republicofbulgaria;THA TH Thailand kingdomo' +
  'fthailand|thailand;SMR SM San_Marino republicofsanmarino|sanmarino;HTI HT Haiti haiti|republ' +
  'icofhaiti;DOM DO Dominican_Rep. dominicanrep|dominicanrepublic;TCD TD Chad chad|republicofch' +
  'ad;KWT KW Kuwait kuwait|stateofkuwait;SLV SV El_Salvador elsalvador|republicofelsalvador;GTM' +
  ' GT Guatemala guatemala|republicofguatemala;TLS TL Timor-Leste democraticrepublicoftimorlest' +
  'e|easttimor|timorleste;BRN BN Brunei brunei|bruneidarussalam|negarabruneidarussalam;MCO MC M' +
  'onaco monaco|principalityofmonaco;DZA DZ Algeria algeria|peoplesdemocraticrepublicofalgeria;' +
  'MOZ MZ Mozambique mozambique|republicofmozambique;SWZ SZ eSwatini eswatini|kingdomofeswatini' +
  '|swaziland;BDI BI Burundi burundi|republicofburundi;RWA RW Rwanda republicofrwanda|rwanda;MM' +
  'R MM Myanmar burma|myanmar|republicoftheunionofmyanmar;BGD BD Bangladesh bangladesh|peoplesr' +
  'epublicofbangladesh;AND AD Andorra andorra|principalityofandorra;AFG AF Afghanistan afghanis' +
  'tan|islamicstateofafghanistan;MNE ME Montenegro montenegro;BIH BA Bosnia_and_Herz. bosniaand' +
  'herz|bosniaandherzegovina;UGA UG Uganda republicofuganda|uganda;USG -- USNB_Guantanamo_Bay g' +
  'itmogtmo|guantanamobayusnb|usnavalbaseguantanamobay|usnbguantanamobay;CUB CU Cuba cuba|repub' +
  'licofcuba;HND HN Honduras honduras|republicofhonduras;ECU EC Ecuador ecuador|republicofecuad' +
  'or;COL CO Colombia colombia|republicofcolombia;PRY PY Paraguay paraguay|republicofparaguay;P' +
  'RT PT Portugal portugal|portugueserepublic;MDA MD Moldova moldova|republicofmoldova;TKM TM T' +
  'urkmenistan turkmenistan;JOR JO Jordan hashemitekingdomofjordan|jordan;NPL NP Nepal nepal;LS' +
  'O LS Lesotho kingdomoflesotho|lesotho;CMR CM Cameroon cameroon|republicofcameroon;GAB GA Gab' +
  'on gabon|gaboneserepublic;NER NE Niger niger|republicofniger;BFA BF Burkina_Faso burkinafaso' +
  ';TGO TG Togo togo|togoleserepublic;GHA GH Ghana ghana|republicofghana;GNB GW Guinea-Bissau g' +
  'uineabissau|republicofguineabissau;GIB GI Gibraltar gibraltar;USA US United_States_of_Americ' +
  'a unitedstates|unitedstatesofamerica;CAN CA Canada canada;MEX MX Mexico mexico|unitedmexican' +
  'states;BLZ BZ Belize belize;PAN PA Panama panama|republicofpanama;VEN VE Venezuela bolivaria' +
  'nrepublicofvenezuela|venezuela|venezuelarb;PNG PG Papua_New_Guinea independentstateofpapuane' +
  'wguinea|papuanewguinea;EGY EG Egypt arabrepublicofegypt|egypt|egyptarabrep;YEM YE Yemen repu' +
  'blicofyemen|yemen|yemenrep;MRT MR Mauritania islamicrepublicofmauritania|mauritania;GNQ GQ E' +
  'q._Guinea eqguinea|equatorialguinea|republicofequatorialguinea;GMB GM Gambia gambia|gambiath' +
  'e|republicofthegambia|thegambia;HKG HK Hong_Kong hongkong|hongkongsar|hongkongsarchina|hongk' +
  'ongspecialadministrativeregionprc;VAT VA Vatican holysee|holyseevaticancity|stateofthevatica' +
  'ncity|vatican|vaticanholysee;CYN -- N._Cyprus cyprusnorthern|ncyprus|northerncyprus|turkishr' +
  'epublicofnortherncyprus;CNM -- Cyprus_U.N._Buffer_Zone cyprusnomansarea|cyprusnomansland|cyp' +
  'rusunbufferzone;KAS -- Siachen_Glacier kashmir|siachenglacier;WSB -- Akrotiri akrotiri|akrot' +
  'irisovereignbasearea;SPI -- Southern_Patagonian_Ice_Field southernpatagonianicefield;BRT -- ' +
  'Bir_Tawil birtawil;ATA AQ Antarctica antarctica;AUS AU Australia ashmoreandcartieris|ashmore' +
  'andcartierislands|australia|commonwealthofaustralia|coralseais|coralseaislands|coralseaislan' +
  'dsterritory|indianoceanter|indianoceanterritories|territoryofashmoreandcartierislands;GRL GL' +
  ' Greenland greenland;FJI FJ Fiji fiji|republicoffiji;NZL NZ New_Zealand newzealand;NCL NC Ne' +
  'w_Caledonia newcaledonia;MDG MG Madagascar madagascar|republicofmadagascar;PHL PH Philippine' +
  's philippines|republicofthephilippines;LKA LK Sri_Lanka democraticsocialistrepublicofsrilank' +
  'a|srilanka;CUW CW Curaçao curacao;ABW AW Aruba aruba;BHS BS Bahamas bahamas|bahamasthe|commo' +
  'nwealthofthebahamas|thebahamas;TCA TC Turks_and_Caicos_Is. turksandcaicosis|turksandcaicosis' +
  'lands;TWN CN-TW Taiwan taiwan;JPN JP Japan japan;SPM PM St._Pierre_and_Miquelon saintpierrea' +
  'ndmiquelon|stpierreandmiquelon;ISL IS Iceland iceland|republicoficeland;PCN PN Pitcairn_Is. ' +
  'pitcairnhendersonducieandoenoislands|pitcairnis|pitcairnislands;PYF PF Fr._Polynesia frenchp' +
  'olynesia|frpolynesia;ATF TF Fr._S._Antarctic_Lands frenchsouthernandantarcticlands|frsandant' +
  'arcticlands|frsantarcticlands|territoryofthefrenchsouthernandantarcticlands;SYC SC Seychelle' +
  's republicofseychelles|seychelles;KIR KI Kiribati kiribati|republicofkiribati;MHL MH Marshal' +
  'l_Is. marshallis|marshallislands|republicofthemarshallislands;TTO TT Trinidad_and_Tobago rep' +
  'ublicoftrinidadandtobago|trinidadandtobago;GRD GD Grenada grenada;VCT VC St._Vin._and_Gren. ' +
  'saintvincentandthegrenadines|stvinandgren|stvincentandthegrenadines;BRB BB Barbados barbados' +
  ';LCA LC Saint_Lucia saintlucia|stlucia;DMA DM Dominica commonwealthofdominica|dominica;UMI U' +
  'M U.S._Minor_Outlying_Is. unitedstatesminoroutlyingislands|usminoroutlyingis;MSR MS Montserr' +
  'at montserrat;ATG AG Antigua_and_Barb. antiguaandbarb|antiguaandbarbuda;KNA KN St._Kitts_and' +
  '_Nevis federationofsaintkittsandnevis|saintkittsandnevis|stkittsandnevis;VIR VI U.S._Virgin_' +
  'Is. unitedstatesvirginislands|usvirginis|virginislands|virginislandsoftheunitedstates|virgin' +
  'islandsus;BLM BL St-Barthélemy saintbarthelemy|stbarthelemy;PRI PR Puerto_Rico commonwealtho' +
  'fpuertorico|puertorico;AIA AI Anguilla anguilla;VGB VG British_Virgin_Is. britishvirginis|br' +
  'itishvirginislands;JAM JM Jamaica jamaica;CYM KY Cayman_Is. caymanis|caymanislands;BMU BM Be' +
  'rmuda bermuda|thebermudasorsomersisles;HMD HM Heard_I._and_McDonald_Is. heardiandmcdonaldis|' +
  'heardiandmcdonaldislands|heardislandandmcdonaldislands|territoryofheardislandandmcdonaldisla' +
  'nds;SHN SH Saint_Helena sainthelena|sainthelenaascensionandtristandacunha|sthelena;MUS MU Ma' +
  'uritius mauritius|republicofmauritius;COM KM Comoros comoros|unionofthecomoros;STP ST São_To' +
  'mé_and_Principe democraticrepublicofsaotomeandprincipe|saotomeandprincipe;CPV CV Cabo_Verde ' +
  'caboverde|republicofcaboverde;MLT MT Malta malta|republicofmalta;JEY JE Jersey bailiwickofje' +
  'rsey|jersey;GGY GG Guernsey bailiwickofguernsey|guernsey;IMN IM Isle_of_Man isleofman;ALA AX' +
  ' Åland aland|alandislands;FRO FO Faeroe_Is. faeroeis|faeroeislands|faroeislands|froyarisfaer' +
  'oeis;IOT IO Br._Indian_Ocean_Ter. brindianoceanter|britishindianoceanterritory;SGP SG Singap' +
  'ore republicofsingapore|singapore;NFK NF Norfolk_Island norfolkisland|territoryofnorfolkisla' +
  'nd;COK CK Cook_Is. cookis|cookislands;TON TO Tonga kingdomoftonga|tonga;WLF WF Wallis_and_Fu' +
  'tuna_Is. wallisandfutuna|wallisandfutunais|wallisandfutunaislands;WSM WS Samoa independentst' +
  'ateofsamoa|samoa;SLB SB Solomon_Is. solomonis|solomonislands;TUV TV Tuvalu tuvalu;MDV MV Mal' +
  'dives maldives|republicofmaldives;NRU NR Nauru nauru|republicofnauru;FSM FM Micronesia feder' +
  'atedstatesofmicronesia|micronesia|micronesiafederatedstatesof;SGS GS S._Geo._and_the_Is. sge' +
  'oandtheis|southgeorgiaandtheislands;FLK FK Falkland_Is. falklandis|falklandislands|falklandi' +
  'slandsislasmalvinas|falklandislandsmalvinas|islasmalvinas;VUT VU Vanuatu republicofvanuatu|v' +
  'anuatu;NIU NU Niue niue;ASM AS American_Samoa americansamoa;PLW PW Palau palau|republicofpal' +
  'au;GUM GU Guam guam|territoryofguam;MNP MP N._Mariana_Is. commonwealthofthenorthernmarianais' +
  'lands|nmarianais|northernmarianaislands;BHR BH Bahrain bahrain|kingdomofbahrain;PGA -- Sprat' +
  'ly_Is. spratlyis|spratlyislands;MAC MO Macao macao|macaosar|macaosarchina|macaospecialadmini' +
  'strativeregionprc|macau;BJN -- Bajo_Nuevo_Bank bajonuevobank|bajonuevobankpetrelis|bajonuevo' +
  'bankpetrelislands;SER -- Serranilla_Bank serranillabank;SCR -- Scarborough_Reef scarboroughr' +
  'eef';

var ISO = null;
function isoTable(){
  if (ISO) return ISO;
  ISO = { byAlias: {}, byIso2: {}, name: {}, all: [] };
  ISO_RAW.split(';').forEach(function (rec) {
    var p = rec.split(' ');
    if (p.length < 4) return;
    var iso3 = p[0], iso2 = p[1], disp = p[2].replace(/_/g, ' '), aliases = p[3];
    ISO.name[iso3] = disp;
    ISO.all.push(iso3);
    if (iso2 !== '--') ISO.byIso2[iso2] = iso3;
    ISO.byAlias[bare(iso3)] = iso3;
    aliases.split('|').forEach(function (a) { if (a && !(a in ISO.byAlias)) ISO.byAlias[a] = iso3; });
  });
  /* What people actually type, which no official list carries. */
  var EXTRA = { uk:'GBR', greatbritain:'GBR', britain:'GBR', england:'GBR', scotland:'GBR', wales:'GBR',
    northernireland:'GBR', gb:'GBR', usa:'USA', us:'USA', america:'USA', unitedstates:'USA',
    unitedstatesofamerica:'USA', holland:'NLD', eire:'IRL', burma:'MMR', ivorycoast:'CIV',
    southkorea:'KOR', korea:'KOR', northkorea:'PRK', uae:'ARE', emirates:'ARE', czechrepublic:'CZE',
    czech:'CZE', russia:'RUS', vatican:'VAT', swaziland:'SWZ', macedonia:'MKD', turkey:'TUR',
    capeverde:'CPV', eastimor:'TLS', laos:'LAO', syria:'SYR', vietnam:'VNM', brunei:'BRN',
    moldova:'MDA', tanzania:'TZA', venezuela:'VEN', bolivia:'BOL', iran:'IRN', hongkong:'HKG',
    macau:'MAC', macao:'MAC', taiwan:'TWN', palestine:'PSE', roi:'IRL',
    /* Endonyms, which turn up in client data more often than the English name. */
    deutschland:'DEU', espana:'ESP', italia:'ITA', nippon:'JPN', nihon:'JPN',
    osterreich:'AUT', suisse:'CHE', schweiz:'CHE', svizzera:'CHE', belgie:'BEL',
    belgique:'BEL', brasil:'BRA', nederland:'NLD', danmark:'DNK', sverige:'SWE',
    norge:'NOR', suomi:'FIN', polska:'POL', magyarorszag:'HUN', ellada:'GRC' };
  Object.keys(EXTRA).forEach(function (k) { ISO.byAlias[k] = EXTRA[k]; });
  /* Placeholders. These are not countries and must not resolve to one — they
     belong in the unresolved bucket where they can be seen and fixed. */
  ISO.blanks = { na:1, none:1, unknown:1, notknown:1, nil:1, 'null':1, tbc:1, tbd:1,
                 xx:1, zz:1, '':1, '0':1, other:1, various:1, nofixedabode:1 };
  return ISO;
}

/* The estate's own Country table is the authority for this project — a code
   column the client actually uses beats any bundled list. Rows are whatever
   the reference table returned; the code and name columns are found by name. */
function geoSeedFromCountryTable(rows){
  var seed = {};
  (rows || []).forEach(function (r) {
    var code = null, name = null;
    Object.keys(r).forEach(function (k) {
      var b = bare(k);
      if (!code && /(^|_)?(code|iso|iso2|iso3|alpha2|alpha3|abbrev)/.test(b)) code = r[k];
      if (!name && /name|description|title|country/.test(b) && !/code|id$/.test(b)) name = r[k];
    });
    if (!code && !name) return;
    var iso3 = geoNormaliseCountry(code) || geoNormaliseCountry(name);
    if (!iso3) return;
    [code, name].forEach(function (v) {
      var b = bare(v);
      if (b) seed[b] = iso3;
    });
  });
  return seed;
}

/* value → ISO-3166-1 alpha-3, or null when it does not resolve. Never a
   guess: an unresolved value is a data-quality finding, not a blank. */
function geoNormaliseCountry(value, seed){
  var b = bare(value);
  if (!b) return null;
  var t = isoTable();
  if (t.blanks[b]) return null;
  if (seed && seed[b]) return seed[b];
  if (b.length === 2 && t.byIso2[b.toUpperCase()]) return t.byIso2[b.toUpperCase()];
  if (b.length === 3 && t.name[b.toUpperCase()]) return b.toUpperCase();
  return t.byAlias[b] || null;
}
function geoCountryName(iso3){ return isoTable().name[iso3] || iso3; }

/* ---------- postcodes ----------------------------------------------------
   GB and US only, and only as far as admin-1. Street-level geocoding is out
   of scope and a postcode district is not a coordinate. */
var GB_AREAS = {
  AB:'Scotland', DD:'Scotland', DG:'Scotland', EH:'Scotland', FK:'Scotland', G:'Scotland',
  HS:'Scotland', IV:'Scotland', KA:'Scotland', KW:'Scotland', KY:'Scotland', ML:'Scotland',
  PA:'Scotland', PH:'Scotland', TD:'Scotland', ZE:'Scotland',
  BT:'Northern Ireland',
  CF:'Wales', LD:'Wales', LL:'Wales', NP:'Wales', SA:'Wales', SY:'Wales',
};
var US_ZIP_STATE = [
  [995,999,'AK'], [350,352,'AL'], [354,369,'AL'], [716,729,'AR'], [850,865,'AZ'], [889,961,'CA'],
  [800,816,'CO'], [60,69,'CT'], [200,205,'DC'], [197,199,'DE'], [320,349,'FL'], [300,319,'GA'],
  [967,968,'HI'], [500,528,'IA'], [832,838,'ID'], [600,629,'IL'], [460,479,'IN'], [660,679,'KS'],
  [400,427,'KY'], [700,715,'LA'], [10,27,'MA'], [206,219,'MD'], [39,49,'ME'], [480,499,'MI'],
  [550,567,'MN'], [630,658,'MO'], [386,397,'MS'], [590,599,'MT'], [270,289,'NC'], [580,588,'ND'],
  [680,693,'NE'], [30,38,'NH'], [70,89,'NJ'], [870,884,'NM'], [889,898,'NV'], [100,149,'NY'],
  [430,459,'OH'], [730,749,'OK'], [970,979,'OR'], [150,196,'PA'], [28,29,'RI'], [290,299,'SC'],
  [570,577,'SD'], [370,385,'TN'], [750,799,'TX'], [840,847,'UT'], [220,246,'VA'], [50,59,'VT'],
  [980,994,'WA'], [530,549,'WI'], [247,268,'WV'], [820,831,'WY'],
];
function geoPostcodeAdmin1(postcode, iso3){
  var s = String(postcode || '').trim().toUpperCase();
  if (!s) return null;
  if (iso3 === 'GBR') {
    var m = s.match(/^([A-Z]{1,2})[0-9]/);
    if (!m) return null;
    return GB_AREAS[m[1]] || 'England';   /* the residual area is England */
  }
  if (iso3 === 'USA') {
    var z = s.match(/^([0-9]{5})/);
    if (!z) return null;
    var p = parseInt(z[1].slice(0, 3), 10);
    for (var i = 0; i < US_ZIP_STATE.length; i++) {
      if (p >= US_ZIP_STATE[i][0] && p <= US_ZIP_STATE[i][1]) return US_ZIP_STATE[i][2];
    }
    return null;
  }
  return null;
}

/* ---------- folding results into the ledger ------------------------------ */
function blankBucket(){ return { n: 0, tables: {}, domains: {} }; }
function bump(bucket, entry, n){
  bucket.n += n;
  bucket.tables[entry.table] = (bucket.tables[entry.table] || 0) + n;
  bucket.domains[entry.domain] = (bucket.domains[entry.domain] || 0) + n;
}

/* rows come back as { country, state, county, postcode, n }. */
function geoFold(results, opts){
  opts = opts || {};
  var seed = opts.seed || null;
  var out = {
    countries: {},          /* ISO3 → bucket, with admin1/admin2 inside */
    unresolved: { n: 0, tables: {}, domains: {}, examples: [], seen: {} },
    total: 0, resolved: 0, tables: 0, queried: 0,
  };
  (results || []).forEach(function (res) {
    if (!res || !res.entry) return;
    out.queried++;
    var entry = res.entry, any = false;
    (res.rows || []).forEach(function (row) {
      var n = Number(row.n || row.N || 0) || 0;
      if (n <= 0) return;
      out.total += n;
      any = true;
      var iso3 = geoNormaliseCountry(row.country, seed);
      /* A postcode can name the country when the country column cannot. */
      if (!iso3 && row.postcode && opts.postcodeCountry) iso3 = opts.postcodeCountry;
      if (!iso3) {
        var u = out.unresolved;
        bump(u, entry, n);
        var raw = row.country == null || row.country === '' ? '(blank)' : String(row.country);
        if (!u.seen[raw]) { u.seen[raw] = 0; }
        u.seen[raw] += n;
        return;
      }
      out.resolved += n;
      var c = out.countries[iso3] || (out.countries[iso3] = {
        iso3: iso3, name: geoCountryName(iso3), n: 0, tables: {}, domains: {}, admin1: {}, admin2: {} });
      bump(c, entry, n);

      var a1 = row.state && String(row.state).trim();
      if (!a1 && row.postcode) a1 = geoPostcodeAdmin1(row.postcode, iso3);
      if (a1) {
        var b1 = c.admin1[a1] || (c.admin1[a1] = Object.assign(blankBucket(), { name: a1, admin2: {} }));
        bump(b1, entry, n);
        var a2 = row.county && String(row.county).trim();
        if (a2) {
          var b2 = b1.admin2[a2] || (b1.admin2[a2] = Object.assign(blankBucket(), { name: a2 }));
          bump(b2, entry, n);
        }
      }
    });
    if (any) out.tables++;
  });

  var u = out.unresolved;
  u.examples = Object.keys(u.seen).sort(function (a, b) { return u.seen[b] - u.seen[a]; })
    .slice(0, 12).map(function (v) { return { value: v, n: u.seen[v] }; });
  out.countryList = Object.keys(out.countries).map(function (k) { return out.countries[k]; })
    .sort(function (a, b) { return b.n - a.n; });
  return out;
}

/* Regions below the threshold roll up into their parent rather than being
   plotted. Off unless asked for: it changes the totals on screen, and a
   number that quietly disagrees with the Data map is worse than a small bar. */
function geoSuppress(ledger, minCell){
  if (!minCell || minCell < 2) return ledger;
  ledger.countryList.forEach(function (c) {
    Object.keys(c.admin1).forEach(function (k) {
      var a1 = c.admin1[k];
      Object.keys(a1.admin2).forEach(function (k2) {
        if (a1.admin2[k2].n < minCell) delete a1.admin2[k2];
      });
      if (a1.n < minCell) delete c.admin1[k];
    });
  });
  ledger.suppressed = minCell;
  return ledger;
}

/* ---------- running the plan --------------------------------------------
   The caller supplies exec(sql) → Promise<rows>. Concurrency is capped
   because some of these tables are large and the console shares one
   connection; a failure is recorded against its table and never sinks the
   rest of the map. */
function geoRun(index, opts){
  opts = opts || {};
  var exec = opts.exec;
  if (typeof exec !== 'function') return Promise.reject(new Error('geoRun needs an exec function'));
  var plan = geoPlan(index, opts);
  var cap = Math.max(1, opts.concurrency || 3);
  var results = new Array(plan.length);
  var failures = [];
  var next = 0, done = 0;

  return new Promise(function (resolve) {
    if (!plan.length) return resolve(finish());
    function finish(){
      var ledger = geoFold(results.filter(Boolean), opts);
      ledger.failures = failures;
      ledger.planned = plan.length;
      if (opts.minCell) geoSuppress(ledger, opts.minCell);
      return ledger;
    }
    function step(){
      if (opts.signal && opts.signal.aborted) { if (done + (plan.length - next) >= plan.length) resolve(finish()); return; }
      if (next >= plan.length) return;
      var i = next++;
      var job = plan[i];
      Promise.resolve()
        .then(function () { return exec(job.sql, job); })
        .then(function (rows) { results[i] = { entry: job.entry, rows: rows || [] }; },
              function (err) { failures.push({ table: job.table, reason: (err && err.message) || String(err) }); })
        .then(function () {
          done++;
          if (opts.onProgress) { try { opts.onProgress(done, plan.length, job.table); } catch (e) {} }
          if (done === plan.length) resolve(finish());
          else step();
        });
    }
    for (var k = 0; k < Math.min(cap, plan.length); k++) step();
  });
}

return {
  geoColumnRole: geoColumnRole,
  isShadowTable: isShadowTable,
  isCountryRef: isCountryRef,
  geoIndex: geoIndex,
  geoSql: geoSql,
  geoPlan: geoPlan,
  geoRun: geoRun,
  geoFold: geoFold,
  geoSuppress: geoSuppress,
  geoNormaliseCountry: geoNormaliseCountry,
  geoSeedFromCountryTable: geoSeedFromCountryTable,
  geoCountryName: geoCountryName,
  geoPostcodeAdmin1: geoPostcodeAdmin1,
  isoCount: function(){ return isoTable().all.length; },
  SHADOW: SHADOW,
};
});
