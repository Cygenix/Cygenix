// Execute Migration (project-builder) — unsaved-changes guard and the
// "Create task" regeneration reminder.
//
// The page already warned about unsaved changes on two paths (switching
// project, opening a job to edit) but not on the ordinary ways out — a
// sidebar link, the back button, closing the tab — and only Save writes the
// group layout, ordering and SQL steps to storage.
//
// Separately, a composite task is a SNAPSHOT of the steps at creation time.
// Editing the jobs afterwards leaves the saved task running the old set with
// nothing to say so; the button now pulses when the two have diverged.
const fs = require('fs');
const vm = require('vm');

const app  = fs.readFileSync(__dirname + '/../public/project-builder-app.js', 'utf8');
const html = fs.readFileSync(__dirname + '/../public/project-builder.html', 'utf8');

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

console.log('Execute Migration — save guard and task-staleness reminder\n');

// ── The staleness rule, run from the shipped source ─────────────────────────
{
  const grab = (name) => {
    const i = app.indexOf('function ' + name + '(');
    if (i < 0) return null;
    // Functions here are small and end at a line-start brace.
    const j = app.indexOf('\n}', i);
    return j < 0 ? null : app.slice(i, j + 2);
  };
  const src = ['currentTaskSignature', 'readTaskSigs', 'rememberTaskSignature', 'isTaskStale']
    .map(grab);
  if (src.some(x => !x)) { check('staleness helpers are present', false, 'could not extract'); }
  else {
    const store = {};
    const sb = {
      JSON, Object,
      localStorage: {
        getItem: k => (k in store ? store[k] : null),
        setItem: (k, v) => { store[k] = String(v); },
      },
      project: null,
      TASK_SIG_KEY: 'cygenix_pb_task_sig',
      refreshTaskStaleness: () => {},
      document: { getElementById: () => null },
    };
    vm.createContext(sb);
    vm.runInContext(src.join('\n'), sb);
    const set = (groups, id) => { sb.project = { id: id || 'p1', name: 'Demo', groups }; };
    const stale = () => vm.runInContext('isTaskStale()', sb);
    const remember = () => vm.runInContext('rememberTaskSignature()', sb);

    const G = (steps, mode) => [{ id: 'g1', mode: mode || 'sequential', steps }];
    const J = (id) => ({ jobId: id, name: id, jobType: 'simple-map' });

    set(G([J('a')]));
    check('with no task ever created there is nothing to regenerate', stale() === false);

    remember();
    check('right after creating a task it is current', stale() === false);

    set(G([J('a'), J('b')]));
    check('adding a job makes the saved task stale', stale() === true);

    remember();
    check('regenerating clears the reminder', stale() === false);

    set(G([J('b'), J('a')]));
    check('reordering jobs makes it stale — order changes what runs', stale() === true);

    remember();
    set(G([J('b'), J('a')], 'parallel'));
    check('changing the group mode makes it stale', stale() === true);

    remember();
    sb.project.name = 'Renamed';
    check('renaming the project does NOT — it changes nothing about the run', stale() === false);

    // Each project keeps its own answer.
    remember();
    set(G([J('z')]), 'p2');
    check('a project with no task of its own is not stale', stale() === false);
    set(G([J('b'), J('a')], 'parallel'), 'p1');
    check('and the first project is still current', stale() === false);
  }
}

// ── Wiring ──────────────────────────────────────────────────────────────────
{
  check('leaving the page with unsaved changes is guarded',
    /addEventListener\('beforeunload'/.test(app) && /if \(!isDirty\) return;/.test(app));
  check('in-app links ask properly and offer to save',
    /a\[href\]/.test(app) && /try \{ saveProject\(\); \} catch \(_\) \{\}\n\s*window\.location\.href = dest\.href;/.test(app));
  check('the link guard runs in capture phase, before other handlers',
    /\}, true\);/.test(app));
  check('external and same-page links are left alone',
    /dest\.origin !== window\.location\.origin/.test(app)
    && /dest\.pathname === window\.location\.pathname/.test(app));
  check('a clean page never prompts', /if \(!isDirty\) return;/.test(app));

  check('creating a task records the fingerprint that clears the flash',
    /rememberTaskSignature\(\);\n\s*showToast\('Task packaged/.test(app));
  check('editing the jobs re-evaluates staleness',
    /function markDirty\(\)[\s\S]{0,220}refreshTaskStaleness\(\);/.test(app));

  check('the button can be targeted', /id="create-task-btn"/.test(html));
  check('and pulses in the app amber when stale',
    /#create-task-btn\.needs-regen/.test(html) && /cygRegenPulse/.test(html)
    && /var\(--amber\)/.test(html));
  check('the pulse is disabled for reduced-motion users',
    /prefers-reduced-motion[\s\S]{0,140}#create-task-btn\.needs-regen \{ animation: none/.test(html));
  check('the tooltip explains why it is flashing',
    /changed since you last created a task/.test(app));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
