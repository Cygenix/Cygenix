// Tests the Object Mapping editor's table picker modal.
//
// The inline search dropdown is positioned inside .panel, which sets
// overflow:hidden for its rounded corners — so results were clipped to
// whatever room was left below the input, often a single row, and finding a
// table by typing was near impossible. The magnifier is now a real button that
// opens every table in a scrollable modal.
//
// The module is extracted from public/object_mapping.html and run against a
// DOM stub, so what is under test is the shipped code.
const fs = require('fs');
const vm = require('vm');

const html = fs.readFileSync(__dirname + '/../public/object_mapping.html', 'utf8');
const start = html.indexOf('// ── Table picker modal ─');
const end   = html.indexOf('async function selectTable(which, value){');
if (start < 0 || end < 0 || end < start) {
  console.log('FAIL: could not locate the table picker module in object_mapping.html');
  process.exit(1);
}

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

const T = (label, rows) => ({ value: label, label, fullName: label, rowCount: rows });
const SRC = [T('dbo.addresses',200), T('dbo.Cases',1500), T('dbo.Address_DM',80),
             T('dbo.Timekeeper_DM',12), T('dbo.TkprRate_DM',4), T('dbo.CaseNotes',9), T('dbo.Users',31)];
const TGT = [T('dbo.cases',0), T('dbo.addresses',0)];

function build({ srcAllTables = SRC, tgtAllTables = TGT, srcTable = null, tgtTable = null } = {}) {
  const els = {};
  const mk = id => (els[id] = {
    id, value:'', textContent:'', innerHTML:'', placeholder:'',
    _classes:new Set(),
    classList:{ add(c){ els[id]._classes.add(c); }, remove(c){ els[id]._classes.delete(c); },
                contains(c){ return els[id]._classes.has(c); } },
    style:{ display:'' }, focus(){ els[id].focused = true; }, select(){},
  });
  ['tbl-picker-modal','tbl-picker-title','tbl-picker-sub','tbl-picker-filter','tbl-picker-list',
   'src-table-input','tgt-table-input','otm-tgt-input','src-drop','tgt-drop','otm-tgt-drop'].forEach(mk);

  const selected = [];
  const sb = {
    console, JSON, String, Array, Number,
    $: id => els[id] || null,
    esc: s => String(s == null ? '' : s).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])),
    escAttr: s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),
    selectTable: (w, v) => { selected.push([w, v]); },
    document: { addEventListener: () => {} },
    srcAllTables, tgtAllTables, srcTable, tgtTable,
  };
  vm.createContext(sb);
  vm.runInContext(html.slice(start, end), sb);
  return { sb, els, selected, get: e => vm.runInContext(e, sb) };
}

// `class="tp-rows"` (the row-count span) is a prefix match for `tp-row`, so
// anchor on the character that follows or every row counts twice.
const rowCount = els => (els['tbl-picker-list'].innerHTML.match(/class="tp-row[ "]/g) || []).length;

console.log('Object Mapping — table picker\n');

// ── Opening ──────────────────────────────────────────────────────────────
let t = build();
t.get("openTablePicker('src')");
check('the picker opens', t.els['tbl-picker-modal'].classList.contains('open'));
check('it is titled for the side it was opened from',
  t.els['tbl-picker-title'].textContent === 'Source tables');
check('every table is listed, not the inline list\'s first few',
  rowCount(t.els) === 7, String(rowCount(t.els)));
check('the count is stated', /7 tables/.test(t.els['tbl-picker-sub'].textContent),
  t.els['tbl-picker-sub'].textContent);
check('row counts are shown', /1,500 rows/.test(t.els['tbl-picker-list'].innerHTML));
check('the filter box takes focus', t.els['tbl-picker-filter'].focused === true);
check('the filter placeholder names the total',
  /Filter 7 tables/.test(t.els['tbl-picker-filter'].placeholder),
  t.els['tbl-picker-filter'].placeholder);

// The inline dropdown must not float above the overlay.
t = build();
t.els['src-drop'].style.display = 'block';
t.get("openTablePicker('src')");
check('opening the picker closes the inline dropdown',
  t.els['src-drop'].style.display === 'none');

t = build();
t.get("openTablePicker('tgt')");
check('the target side is titled correctly',
  t.els['tbl-picker-title'].textContent === 'Target tables');
check('the target side lists target tables', rowCount(t.els) === 2);

// One-to-many adds target tables, so it reads the same list.
t = build();
t.get("openTablePicker('otm-tgt')");
check('one-to-many reads the target table list', rowCount(t.els) === 2);

// ── Carrying over what was typed inline ──────────────────────────────────
t = build();
t.els['src-table-input'].value = '  Case ';
t.get("openTablePicker('src')");
check('text typed inline carries into the filter',
  t.els['tbl-picker-filter'].value === 'Case', t.els['tbl-picker-filter'].value);
check('the carried filter is applied immediately', rowCount(t.els) === 2, String(rowCount(t.els)));
check('the subtitle reports the filtered count',
  /2 of 7 tables/.test(t.els['tbl-picker-sub'].textContent), t.els['tbl-picker-sub'].textContent);

// ── Filtering ────────────────────────────────────────────────────────────
t = build();
t.get("openTablePicker('src')");
t.els['tbl-picker-filter'].value = 'dm';
t.get('renderTablePicker()');
check('filtering is case-insensitive', rowCount(t.els) === 3, String(rowCount(t.els)));
check('the match is highlighted', /tp-mark/.test(t.els['tbl-picker-list'].innerHTML));

t.els['tbl-picker-filter'].value = 'zzz';
t.get('renderTablePicker()');
check('no match says so rather than showing an empty box',
  rowCount(t.els) === 0 && /No table matches/.test(t.els['tbl-picker-list'].innerHTML));

t.els['tbl-picker-filter'].value = '';
t.get('renderTablePicker()');
check('clearing the filter brings everything back', rowCount(t.els) === 7);

// A filter containing HTML must not be injected into the list.
t.els['tbl-picker-filter'].value = '<img src=x onerror=alert(1)>';
t.get('renderTablePicker()');
check('a filter containing markup is escaped',
  !/<img/.test(t.els['tbl-picker-list'].innerHTML),
  t.els['tbl-picker-list'].innerHTML.slice(0, 120));

// ── The current table is marked ──────────────────────────────────────────
t = build({ srcTable: SRC[1] });
t.get("openTablePicker('src')");
check('the table already in use is marked',
  (t.els['tbl-picker-list'].innerHTML.match(/tp-row current/g) || []).length === 1);

t = build();
t.get("openTablePicker('src')");
check('nothing is marked when no table is chosen yet',
  !/tp-row current/.test(t.els['tbl-picker-list'].innerHTML));

// ── Picking ──────────────────────────────────────────────────────────────
t = build();
t.get("openTablePicker('src')");
t.get("pickTableFromPicker('dbo.Cases')");
check('picking selects the table', JSON.stringify(t.selected) === JSON.stringify([['src','dbo.Cases']]),
  JSON.stringify(t.selected));
check('picking closes the modal', !t.els['tbl-picker-modal'].classList.contains('open'));
check('picking mirrors the name into the inline box',
  t.els['src-table-input'].value === 'dbo.Cases', t.els['src-table-input'].value);

t = build();
t.get("openTablePicker('otm-tgt')");
t.get("pickTableFromPicker('dbo.cases')");
check('one-to-many picks against its own input and side',
  JSON.stringify(t.selected) === JSON.stringify([['otm-tgt','dbo.cases']]) &&
  t.els['otm-tgt-input'].value === 'dbo.cases');

// ── Keyboard ─────────────────────────────────────────────────────────────
t = build();
t.get("openTablePicker('src')");
t.els['tbl-picker-filter'].value = 'Timekeeper';
t.get('renderTablePicker()');
t.get("tablePickerKey({ key:'Enter' })");
check('Enter picks when exactly one table matches',
  JSON.stringify(t.selected) === JSON.stringify([['src','dbo.Timekeeper_DM']]),
  JSON.stringify(t.selected));

t = build();
t.get("openTablePicker('src')");
t.els['tbl-picker-filter'].value = 'dm';
t.get('renderTablePicker()');
t.get("tablePickerKey({ key:'Enter' })");
check('Enter does nothing while several tables match', t.selected.length === 0);

t = build();
t.get("openTablePicker('src')");
t.get("tablePickerKey({ key:'Escape' })");
check('Escape closes without picking',
  !t.els['tbl-picker-modal'].classList.contains('open') && t.selected.length === 0);

// ── Nothing connected ────────────────────────────────────────────────────
t = build({ srcAllTables: [] });
t.get("openTablePicker('src')");
check('with no tables loaded it says to connect first',
  /Connect the database first/.test(t.els['tbl-picker-list'].innerHTML));
check('and reports no count', t.els['tbl-picker-sub'].textContent === '');

// ── The clipping fix ─────────────────────────────────────────────────────
// The modal is the answer to "show me everything", but typing inline has to
// work too — it was the actual complaint. The panel holding a search must not
// clip its dropdown.
check('a panel containing a search opts out of overflow clipping',
  /\.panel:has\(\.search-wrap\)\{overflow:visible\}/.test(html));
check('other panels keep their clipping',
  /\.panel\{[^}]*overflow:hidden/.test(html));

// The magnifier has to be a real button — as placeholder text it vanished the
// moment you typed and could never be clicked.
check('the source magnifier is a button that opens the picker',
  /class="search-icon-btn"[^>]*openTablePicker\('src'\)/.test(html));
check('the target magnifier is a button that opens the picker',
  /class="search-icon-btn"[^>]*openTablePicker\('tgt'\)/.test(html));
check('the magnifier is no longer buried in a placeholder',
  !/placeholder="🔍 Search source tables/.test(html));

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
