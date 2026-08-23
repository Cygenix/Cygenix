// Two SQL Server catalogs disagree about what a row count is called:
// sys.partitions has `rows`, sys.dm_db_partition_stats has `row_count`.
// Mixing them compiles nowhere and fails on every server with
// "Invalid column name" — which is exactly how the Quality Review page's
// Row counts check broke. This sweep pins the whole repo so the two can
// never be crossed again in either direction.
const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('DMV column names — sys.partitions vs sys.dm_db_partition_stats\n');

const roots = ['public', 'netlify/functions', 'azure-function/src'];
const files = [];
const walk = (dir) => {
  if (!fs.existsSync(dir)) return;
  for (const f of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, f.name);
    if (f.isDirectory()) { if (f.name !== 'node_modules' && f.name !== 'geo') walk(p); }
    else if (/\.(js|html)$/.test(f.name)) files.push(p);
  }
};
roots.forEach(r => walk(path.join(__dirname, '..', r)));

// Around each mention of either catalog, the near context (same statement,
// give or take) must use that catalog's own column name.
const offenders = [];
let dmvSites = 0, partSites = 0;
for (const file of files) {
  const text = fs.readFileSync(file, 'utf8');
  let m;
  const re = /sys\.(dm_db_partition_stats|partitions)\s+(?:AS\s+)?([a-z]\w*)/gi;
  while ((m = re.exec(text)) !== null) {
    const catalog = m[1].toLowerCase(), alias = m[2];
    const ctx = text.slice(Math.max(0, m.index - 400), m.index + 400);
    const rel = path.relative(path.join(__dirname, '..'), file);
    if (catalog === 'dm_db_partition_stats') {
      dmvSites++;
      if (new RegExp('\\b' + alias + '\\.rows\\b').test(ctx)) {
        offenders.push(rel + ': ' + alias + '.rows against dm_db_partition_stats (column is row_count)');
      }
    } else {
      partSites++;
      if (new RegExp('\\b' + alias + '\\.row_count\\b').test(ctx)) {
        offenders.push(rel + ': ' + alias + '.row_count against sys.partitions (column is rows)');
      }
    }
  }
}
check('every dm_db_partition_stats query uses row_count (' + dmvSites + ' sites)',
  offenders.filter(o => /dm_db_partition_stats/.test(o)).length === 0,
  offenders.join(' | '));
check('every sys.partitions query uses rows (' + partSites + ' sites)',
  offenders.filter(o => /sys\.partitions/.test(o)).length === 0,
  offenders.join(' | '));
check('the sweep actually found both catalogs in use', dmvSites >= 2 && partSites >= 2,
  dmvSites + ' dmv / ' + partSites + ' partitions');

// And the specific line that broke: the Quality Review snapshot.
const dq = fs.readFileSync(path.join(__dirname, '..', 'public', 'data-quality.html'), 'utf8');
check('the Quality Review row-count snapshot sums row_count',
  /SUM\(p\.row_count\)\s+AS \[c\]/.test(dq) && !/SUM\(p\.rows\)/.test(dq));

console.log('\n' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
