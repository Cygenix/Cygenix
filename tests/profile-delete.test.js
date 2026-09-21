// tests/profile-delete.test.js — retired profiles fold away; a retired
// profile nothing references can be deleted, and stays deleted.
//
// WHAT WAS ASKED
// The Profiles register kept every superseded pairing on the working list
// with no way off it. Two things fix that without touching the archive:
// retired rows are hidden by default (the counts still count them), and a
// retired profile with NO bindings and NO run records gets a guarded
// Delete. A retired profile with history is archived, never deleted.
//
// WHAT THIS PINS, by running the real modules
//   1. the engine: eligibility (status, then history), the tombstone the
//      removal leaves, and that saved connections and their classification
//      are not touched;
//   2. the merge (both copies, byte-identical): a tombstone beats an older
//      copy on another machine — the resurrection case — but loses to a
//      run record or binding anywhere in the union, and to a newer copy;
//      commutative, idempotent, and a tombstone-only store is not "empty";
//   3. the Function App: the delete action refuses with 409 unless retired
//      and unreferenced, applies the same words as the engine, tombstones
//      even when the cloud copy lacks the profile, and the save path drops
//      a forged tombstone for a profile the cloud holds as not retired;
//   4. the page: retired rows hidden by default, the toggle remembers per
//      browser in try/catch, the counts still include retired, Delete only
//      on retired rows with the reason as tooltip, the sentinel picker
//      excludes retired, the bind picker is active-only, the typed-id
//      confirmation is the PRD pattern, POSTs are guarded, the cloud goes
//      first, the audit entry has what the brief lists;
//   5. the allow-lists: the proxy forwards the action, the audit schema
//      accepts profile.delete.
'use strict';

const fs = require('fs');
const path = require('path');
const vm = require('vm');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 320) : '')); }
};
const section = (t) => console.log('\n' + t + '\n' + '─'.repeat(t.length));
const ROOT = path.join(__dirname, '..');
const read = (...p) => fs.readFileSync(path.join(ROOT, ...p), 'utf8');
const code = (src) => src.split('\n').filter((l) => !/^\s*(\/\/|\*|\/\*)/.test(l)).join('\n');
const throws = (fn, re) => { try { fn(); return false; } catch (e) { return re ? re.test(String(e.message)) : true; } };

const P = require('../public/cygenix-profiles.js');
const M = require('../public/cygenix-profile-merge.js');

console.log('Connection profiles — hiding retired ones, deleting the unreferenced\n');

const T0 = 1700000000000;
const CONNS = [
  { id: 'c_src', side: 'src', mode: 'direct', name: 'Src', connString: 'Server=a;Database=fin_dev;' },
  { id: 'c_tgt', side: 'tgt', mode: 'direct', name: 'Tgt', connString: 'Server=b;Database=fin_dev_tgt;' },
];
function seeded() {
  const s = P.cpNewStore(T0);
  P.cpSetConnMeta(s, 'c_src', { envClass: 'DEV', systemDomain: 'FIN' }, 'u', T0);
  P.cpSetConnMeta(s, 'c_tgt', { envClass: 'DEV', systemDomain: 'FIN' }, 'u', T0);
  ['FIN-DEV-01', 'FIN-DEV-02', 'FIN-DEV-03'].forEach((id, i) => {
    P.cpSaveProfile(s, { id, name: id, envClass: 'DEV', srcConnId: 'c_src', tgtConnId: 'c_tgt' }, CONNS, 'u', T0 + i);
    P.cpActivateProfile(s, id, CONNS, 'u', T0 + 10 + i);
  });
  return s;
}

/* ── 1. The engine ───────────────────────────────────────────────────────── */
section('1. Engine — eligibility, the tombstone, and what is left alone');
{
  const s = seeded();
  check('a new store carries an empty tombstone list', Array.isArray(P.cpNewStore(1).deleted) && P.cpNewStore(1).deleted.length === 0);
  let e = P.cpDeleteEligibility(s, 'FIN-DEV-01');
  check('an ACTIVE profile is not deletable — status first, in words', !e.ok && e.code === 'status' && /is active — only a retired profile can be deleted/.test(e.why), e.why);
  check('and the engine refuses, not just advises', throws(() => P.cpDeleteProfile(s, 'FIN-DEV-01', 'u', T0 + 50), /only a retired profile/));
  check('a profile that does not exist is refused too', !P.cpDeleteEligibility(s, 'NOPE').ok && P.cpDeleteEligibility(s, 'NOPE').code === 'missing');

  P.cpRetireProfile(s, 'FIN-DEV-01', 'u', T0 + 100);
  P.cpRetireProfile(s, 'FIN-DEV-02', 'u', T0 + 100);
  P.cpBind(s, 'job', 'j1', 'FIN-DEV-02', 'u', T0 + 20);
  P.cpRecordRun(s, { artifactType: 'job', artifactId: 'j1', profileId: 'FIN-DEV-02', outcome: 'ok' }, T0 + 30);
  P.cpRecordRun(s, { artifactType: 'job', artifactId: 'j1', profileId: 'FIN-DEV-02', outcome: 'ok' }, T0 + 31);
  P.cpRecordRun(s, { artifactType: 'job', artifactId: 'j1', profileId: 'FIN-DEV-02', outcome: 'ok' }, T0 + 32);
  check('usage counts bindings and run records by profileId', JSON.stringify(P.cpProfileUsage(s, 'FIN-DEV-02')) === '{"bindings":1,"runs":3}');
  e = P.cpDeleteEligibility(s, 'FIN-DEV-02');
  check('a retired profile WITH history is kept for audit, and the tooltip says what history',
    !e.ok && e.code === 'history' && e.why === 'Kept for audit: 3 run records, 1 binding', e.why);
  check('singular forms read right', P.cpUsageText({ runs: 1, bindings: 1 }) === '1 run record, 1 binding');
  check('a retired profile with NO history is deletable', P.cpDeleteEligibility(s, 'FIN-DEV-01').ok);

  const metaBefore = JSON.stringify(s.connMeta);
  const lockBefore = P.cpIsConnLocked(s, 'c_src') + '/' + P.cpIsConnLocked(s, 'c_tgt');
  const gone = P.cpDeleteProfile(s, 'FIN-DEV-01', 'alice', T0 + 200);
  check('the deletion returns the profile it removed', gone && gone.id === 'FIN-DEV-01' && gone.envClass === 'DEV');
  check('the profile is gone from the list', !s.profiles.some((p) => p.id === 'FIN-DEV-01') && s.profiles.length === 2);
  check('and a tombstone is left: id, when, who', s.deleted.length === 1 && s.deleted[0].id === 'FIN-DEV-01' && s.deleted[0].at === T0 + 200 && s.deleted[0].by === 'alice');
  check('connection classifications are untouched — deleting a profile reclassifies nothing', JSON.stringify(s.connMeta) === metaBefore);
  check('the retired profile never locked its connections, so the lock state is exactly what it was',
    P.cpIsConnLocked(s, 'c_src') + '/' + P.cpIsConnLocked(s, 'c_tgt') === lockBefore && lockBefore === 'true/true');
  check('an event records it, with name and environment for the trail',
    s.events.some((ev) => ev.type === 'profile.deleted' && ev.profileId === 'FIN-DEV-01' && ev.envClass === 'DEV' && ev.by === 'alice'));
  check('the second delete of the same id is refused as missing', !P.cpDeleteEligibility(s, 'FIN-DEV-01').ok);

  // A merge from another machine can leave a retired profile named as the
  // selection. Deleting it clears the selection without a selection event.
  const s2 = seeded();
  P.cpRetireProfile(s2, 'FIN-DEV-03', 'u', T0 + 100);
  s2.settings.activeProfileId = 'FIN-DEV-03';
  P.cpDeleteProfile(s2, 'FIN-DEV-03', 'u', T0 + 200);
  check('a stale selection naming the deleted profile is cleared', s2.settings.activeProfileId === null);
}

/* ── 2. The merge ────────────────────────────────────────────────────────── */
section('2. Merge — the tombstone survives the union, history survives the tombstone');
{
  check('azure-function/src/profile-merge.js is still byte-for-byte the browser copy',
    read('azure-function', 'src', 'profile-merge.js') === read('public', 'cygenix-profile-merge.js'));

  // THE resurrection case: deleted on machine A; machine B still holds the
  // retired copy from before and uploads it.
  const A = seeded(); P.cpRetireProfile(A, 'FIN-DEV-01', 'u', T0 + 100);
  const B = JSON.parse(JSON.stringify(A));                       // B's copy, taken before the delete
  P.cpDeleteProfile(A, 'FIN-DEV-01', 'u', T0 + 200);
  const AB = M.mergeProfileStores(A, B), BA = M.mergeProfileStores(B, A);
  check('merging with a machine that still holds the profile does NOT bring it back',
    !AB.profiles.some((p) => p.id === 'FIN-DEV-01') && AB.profiles.length === 2, JSON.stringify(AB.profiles.map((p) => p.id)));
  check('whichever side is first', !BA.profiles.some((p) => p.id === 'FIN-DEV-01'));
  check('the tombstone is carried in the union', AB.deleted.length === 1 && AB.deleted[0].id === 'FIN-DEV-01');
  check('idempotent: merging the result with either input changes nothing',
    M.storesEqual(M.mergeProfileStores(AB, B), AB) && M.storesEqual(M.mergeProfileStores(AB, A), AB));
  check('and the cloud copy, merged with itself, is unchanged', M.storesEqual(M.mergeProfileStores(AB, AB), AB));

  // B's copy is OLDER and never saw the retirement (still active). The
  // tombstone is newer than that copy, so it still wins: what B holds is a
  // stale view of a profile that was retired and then deleted.
  const B2 = seeded();                                           // FIN-DEV-01 active, updatedAt T0+10
  const AB2 = M.mergeProfileStores(A, B2);
  check('a stale copy that never saw the retirement is dropped too — the tombstone is newer than it',
    !AB2.profiles.some((p) => p.id === 'FIN-DEV-01'), JSON.stringify(AB2.profiles.map((p) => p.id)));

  // History anywhere in the union keeps the profile and discards the tombstone.
  const C = seeded(); P.cpRetireProfile(C, 'FIN-DEV-01', 'u', T0 + 100);
  P.cpRecordRun(C, { artifactType: 'job', artifactId: 'j9', profileId: 'FIN-DEV-01', outcome: 'ok' }, T0 + 150);
  const AC = M.mergeProfileStores(A, C);
  check('a run record for it on another machine KEEPS the profile — history is archived, never deleted',
    AC.profiles.some((p) => p.id === 'FIN-DEV-01' && p.status === 'retired'), JSON.stringify(AC.profiles.map((p) => p.id)));
  check('and the tombstone that lost is discarded, so it cannot win later', AC.deleted.length === 0, JSON.stringify(AC.deleted));
  const D = seeded(); P.cpRetireProfile(D, 'FIN-DEV-01', 'u', T0 + 100);
  P.cpBind(D, 'job', 'j9', 'FIN-DEV-01', 'u', T0 + 150);
  check('a binding on another machine keeps it the same way', M.mergeProfileStores(A, D).profiles.some((p) => p.id === 'FIN-DEV-01'));

  // The id created again, later: the newer copy wins over the tombstone.
  const E = JSON.parse(JSON.stringify(A));
  P.cpSaveProfile(E, { id: 'FIN-DEV-01', name: 'again', envClass: 'DEV', srcConnId: 'c_src', tgtConnId: 'c_tgt' }, CONNS, 'u', T0 + 300);
  check('cpSaveProfile on a store that holds the tombstone creates the profile again (the tombstone is a merge fact, not a ban)', E.profiles.some((p) => p.id === 'FIN-DEV-01'));
  const AE = M.mergeProfileStores(A, E);
  check('a copy NEWER than the tombstone survives the merge — the id was created again', AE.profiles.some((p) => p.id === 'FIN-DEV-01' && p.name === 'again'));
  check('and that tombstone is gone', AE.deleted.length === 0);

  check('a store holding only a tombstone is NOT empty — it is the one fact the cloud must learn',
    !M.isEmptyStore({ v: 1, deleted: [{ id: 'X', at: 1 }] }) && M.isEmptyStore({ v: 1, deleted: [] }));
  check('canonical form sees a tombstone, so "did the merge change anything" answers yes',
    !M.storesEqual({ v: 1 }, { v: 1, deleted: [{ id: 'X', at: 1 }] }));
  check('a malformed deleted list never throws', !throws(() => M.mergeProfileStores({ deleted: 'x' }, { deleted: [null, {}, { id: 'a' }] })));
  const many = { deleted: Array.from({ length: 260 }, (_, i) => ({ id: 'd' + i, at: i })) };
  const capped = M.mergeProfileStores(many, {});
  check('tombstones are capped at ' + M.DELETED_CAP + ', keeping the newest', capped.deleted.length === M.DELETED_CAP && capped.deleted[0].id === 'd60');
  check('the merge still honours the old contract for everything else — a profile only one side has is kept',
    M.mergeProfileStores(seeded(), P.cpNewStore(1)).profiles.length === 3);
}

/* ── 3. The Function App ─────────────────────────────────────────────────── */
section('3. Function App — the same rule on the cloud copy, and a 409 in words');
{
  const IDX = read('azure-function', 'src', 'index.js');
  const idx = code(IDX);
  // Run the two helpers on their own: the module itself needs Azure.
  const m = IDX.match(/function guardProfileTombstones\(cloudStore, incoming\) \{[\s\S]*?\n\}\n[\s\S]*?function profileDeleteCheck\(store, id\) \{[\s\S]*?\n\}\n/);
  check('index.js defines guardProfileTombstones and profileDeleteCheck at module scope', !!m);
  const ctx = {}; vm.createContext(ctx);
  vm.runInContext((m ? m[0] : '') + '\nthis.guard = guardProfileTombstones; this.checkFn = profileDeleteCheck;', ctx);
  const s = seeded();
  P.cpRetireProfile(s, 'FIN-DEV-01', 'u', T0 + 100);
  P.cpRetireProfile(s, 'FIN-DEV-02', 'u', T0 + 100);
  P.cpBind(s, 'job', 'j1', 'FIN-DEV-02', 'u', T0 + 20);
  P.cpRecordRun(s, { artifactType: 'job', artifactId: 'j1', profileId: 'FIN-DEV-02' }, T0 + 30);
  const server = (id) => ctx.checkFn(s, id), engine = (id) => P.cpDeleteEligibility(s, id);
  ['FIN-DEV-01', 'FIN-DEV-02', 'FIN-DEV-03', 'NOPE'].forEach((id) => {
    const a = server(id), b = engine(id);
    check('server and engine give the same verdict and the same words for ' + id,
      a.ok === b.ok && a.code === b.code && a.why === b.why, a.why + ' vs ' + b.why);
  });
  check('the delete action exists and is POST-only', /case 'connection-profile-delete': \{[\s\S]*?req\.method !== 'POST'/.test(idx));
  const act = (idx.match(/case 'connection-profile-delete': \{[\s\S]*?\n {8}\}/) || [''])[0];
  check('it refuses with 409 and the reason as the message', /return err\(409, check\.why\)/.test(act));
  check('it tombstones a profile the cloud copy does not hold, and says so', /tombstoned: true, profileId \}\)/.test(act) && /deleted: false, tombstoned: true/.test(act));
  check('a stale selection of the deleted profile is cleared on the cloud copy too', /activeProfileId === profileId/.test(act));
  check('it never reads or writes saved connections', !/saved_connections|connections\b(?!_profiles)/.test(act.replace(/connection-profile-delete|connection_profiles/g, '')));
  check('the save path guards tombstones with the cloud copy\'s status before merging', /guardProfileTombstones\(existing\[key\], body\[key\]\)/.test(idx));
  const cloud = { profiles: [{ id: 'A', status: 'active' }, { id: 'R', status: 'retired' }] };
  const g = ctx.guard(cloud, { deleted: [{ id: 'A', at: 1 }, { id: 'R', at: 1 }, { id: 'GONE', at: 1 }] });
  check('a forged tombstone for an active profile is dropped; retired and absent ones stand',
    g.dropped === 1 && g.value.deleted.map((d) => d.id).join() === 'R,GONE', JSON.stringify(g));
  check('and an upload with no tombstones passes through untouched', ctx.guard(cloud, { profiles: [] }).dropped === 0);

  check('the proxy allow-list forwards the action', /'connection-profile-delete'/.test(code(read('netlify', 'functions', 'data-proxy.js'))));
  check('the audit schema accepts profile.delete from the browser, under connections',
    /'profile\.delete':\s*'connections'/.test(code(read('netlify', 'functions', 'lib', 'audit-schema.js'))));
}

/* ── 4. The page ─────────────────────────────────────────────────────────── */
section('4. Page — hidden by default, a remembered toggle, Delete only where it applies');
{
  const HTML = read('public', 'profiles.html');
  const js = code(HTML);
  check('retired rows are filtered out unless the toggle says otherwise', /store\.profiles\.filter\(p => showRetired \|\| p\.status !== 'retired'\)/.test(js));
  check('the summary line still counts retired profiles', /filter\(p => p\.status === 'retired'\)\.length \+ ' retired'/.test(js));
  check('the toggle reads "Show N retired" / "Hide retired" and hides itself at zero',
    /'Show ' \+ retiredCount \+ ' retired'/.test(js) && /'Hide retired'/.test(js) && /if \(retiredCount\) \{[\s\S]*?\} else \{[\s\S]*?display = 'none'/.test(js));
  check('the toggle state is per browser, read and written inside try/catch',
    /try \{ return localStorage\.getItem\('cygenix_profiles_show_retired'\) === '1'; \} catch/.test(js)
    && /try \{ localStorage\.setItem\('cygenix_profiles_show_retired', next\); \} catch/.test(js));
  check('Delete is rendered only for retired rows', /function deleteControl\(p\)\{\s*if \(p\.status !== 'retired'\) return '';/.test(js));
  check('it is styled like Retire, destructive', /<button class="btn btn-danger" onclick="cpDeleteProfile\(/.test(js));
  check('ineligible: disabled, with the engine\'s reason as the tooltip', /<span class="cp-hold" title="' \+ esc\(e\.why\) \+ '"><button class="btn btn-danger" disabled/.test(js));
  check('the sentinel picker excludes retired profiles and falls back to the first active one',
    /function sentinelChoices\(\)\{\s*return store\.profiles\.filter\(p => p\.status !== 'retired'\)/.test(js)
    && /choices\.find\(x => x\.status === 'active'\) \|\| choices\[0\]/.test(js));
  check('the bind-artifact picker lists active profiles only', /\$\('cp-b-profile'\)\.innerHTML = store\.profiles\.filter\(p => p\.status === 'active'\)/.test(js));
  check('the selected radio is checked only for an active profile — a retired selection is shown as none, and nothing is written',
    /store\.settings\.activeProfileId === p\.id && p\.status === 'active' \? 'checked'/.test(js));

  const del = (js.match(/async function cpDeleteProfile\(id\)\{[\s\S]*?\n\}/) || [''])[0];
  check('cpDeleteProfile exists', del.length > 0);
  check('eligibility is re-checked at delete time, from a fresh read of the store',
    (del.match(/store = P\.cpLoad\(\);\s*const (first|again) = P\.cpDeleteEligibility\(store, id\)/g) || []).length === 2);
  check('the confirmation is the PRD pattern: a prompt that must be answered with the exact id',
    /prompt\('Delete profile ' \+ id[\s\S]*?Type the profile id to confirm:'\)/.test(del)
    && /if \(String\(typed\)\.trim\(\) !== id\) \{\s*alert\('Confirmation did not match ' \+ id \+ ' — nothing was changed\.'\)/.test(del));
  check('the dialog states what will be removed and that saved connections are not touched',
    /removes the profile record from this browser and from your account/.test(del) && /Saved connections are not touched/.test(del));
  check('in-flight guard and 3-second minimum interval on the POSTing function',
    /if \(_delInFlight\) return;\s*if \(Date\.now\(\) - _delLastAt < DEL_MIN_INTERVAL_MS\) return;/.test(del) && /const DEL_MIN_INTERVAL_MS = 3000;/.test(js));
  check('the in-flight flag is set once and cleared only in finally — not by any callback',
    /_delInFlight = true;/.test(del) && /finally \{\s*_delInFlight = false;\s*\}/.test(del)
    && (del.match(/_delInFlight = false/g) || []).length === 1);
  check('the cloud copy goes first, and a refusal stops the local delete',
    /callResult\('connection-profile-delete', \{ method: 'POST', body: \{ profileId: id \} \}\)/.test(del)
    && /if \(!r\.ok\) \{[\s\S]*?return;[\s\S]*?\}[\s\S]*?P\.cpDeleteProfile\(store, id/.test(del));
  check('the local removal goes through the engine — the tombstone is the engine\'s', /gone = P\.cpDeleteProfile\(store, id, userName\(\), Date\.now\(\)\)/.test(del));
  check('the audit entry is profile.delete with id, name, env and both standard names; PRD goes under PROD',
    /action: 'profile\.delete', category: 'connections'/.test(del)
    && /detail: \{ profileId: gone\.id, name: gone\.name \|\| '', envClass: gone\.envClass,[\s\S]*?srcStandardName: srcName, tgtStandardName: tgtName/.test(del)
    && /env: gone\.envClass === 'PRD' \? 'PROD' : null/.test(del));
  check('the standard names are read before the profile goes', /const srcName = connLabel\(p\.srcConnId\), tgtName = connLabel\(p\.tgtConnId\);[\s\S]*?P\.cpDeleteProfile/.test(del));
  check('the delete path never writes a connections store', !/cygenix_saved_connections|cygenix_project_connections|savedSetAll|setActive\(/.test(del));
  check('the table, counts and pickers re-render after a delete', /saveStore\(\);\s*renderAll\(\);/.test(del));
  check('Retire is unchanged for draft/active rows', /p\.status !== 'retired' \? '<button class="btn btn-danger" onclick="cpRetire\(/.test(js));
  check('the new key is classified in the storage inventory', /'cygenix_profiles_show_retired':\s*\['C'/.test(read('scripts', 'storage-inventory.js')));
  check('no emoji crept into the page', !/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/u.test(HTML));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
