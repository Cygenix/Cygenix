/* tests/browser/assistant-loop.smoke.js
 * ---------------------------------------------------------------------------
 * The agent loop, end to end, in a real browser.
 *
 * WHY THIS EXISTS SEPARATELY FROM tests/page-reader.test.js
 * That suite proves the rules: what an id is made of, what confirms, what is
 * refused. It proves them against pure functions and against the source, which
 * is the right way to pin a rule — but a rule that is never reached is not a
 * rule. Everything BETWEEN the model saying "click this" and the button moving
 * is wiring: executeAll, the confirmation parking, the trail, the budget, the
 * brakes. Wiring cannot be tested with a regular expression.
 *
 * So this drives the real CygenixAssistant runtime, in a real page, with a
 * scripted model standing in for Claude, and watches what actually happens to
 * the DOM. The single most valuable assertion in the file is the one that
 * proves a click waiting for approval does NOT happen.
 *
 * Not part of `npm test` — it needs a browser, and the deploy build has none.
 * Run it by hand:  node tests/browser/assistant-loop.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8397);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 400) : '')); }
};

/* The screen under test. Ordinary controls, one of each kind the tools touch,
   and a Run button whose handler records that it ran — which is how "the
   confirmation held it" is told apart from "nothing happened". */
const FIXTURE = `<!doctype html><html><head><title>Jobs</title></head><body>
  <h1>Jobs</h1>
  <input id="filter" name="filter" placeholder="Filter jobs">
  <select id="env" name="env"><option>Development</option><option>Production</option></select>
  <button id="search">Search jobs</button>
  <button id="run">Run job</button>
  <div id="out"></div>
  <script>
    window.__ran = [];
    document.getElementById('run').addEventListener('click', function () {
      window.__ran.push('run');
      setTimeout(function () { document.getElementById('out').innerHTML = '<h2>Job started</h2>'; }, 120);
    });
    document.getElementById('search').addEventListener('click', function () { window.__ran.push('search'); });
    try { localStorage.setItem('cygenix_api_key', 'stub-key-for-the-loop-test'); } catch (e) {}

    /* The model, scripted. Each turn is either a plain text reply or a tool
       call; a tool call names its element by LABEL, and the stub resolves that
       to the real id at call time — the test cannot know the hashes up front,
       and hard-coding them would be testing the hash rather than the loop. */
    window.__turns = [];
    window.__asked = 0;
    window.CygenixModel = {
      mdCall: async function () {
        window.__asked++;
        var t = window.__turns.shift();
        if (!t) return { response: { json: async function () {
          return { content: [{ type: 'text', text: 'Done.' }] }; } } };
        var content;
        if (t.text) {
          content = [{ type: 'text', text: t.text }];
        } else {
          var input = t.input || {};
          if (t.label) {
            var last = window.CygenixPageReader.lastRead();
            var id = null;
            if (last) Object.keys(last.entries).forEach(function (k) {
              if (last.entries[k].label === t.label) id = k;
            });
            input = Object.assign({ element_id: id || 'el_00000000' }, input);
          }
          content = [{ type: 'tool_use', id: 'tu_' + window.__asked, name: t.tool, input: input }];
        }
        return { response: { json: async function () { return { content: content }; } } };
      }
    };
  <\/script>
  <script src="/cygenix-page-reader.js"></script>
  <script src="/cygenix-assistant.js"></script>
  <script src="/cygenix-assistant-actions.js"></script>
  <script>CygenixAssistant.registerPage('jobs');<\/script>
</body></html>`;

// charset matters: without it the browser reads UTF-8 source as latin-1 and
// every em dash in a label arrives as mojibake. Netlify sets it; this must too.
const TYPES = { '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
                '.html': 'text/html; charset=utf-8' };
const server = http.createServer((req, res) => {
  const p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/loop') {
    res.writeHead(200, { 'Content-Type': TYPES['.html'] });
    return res.end(FIXTURE);
  }
  const f = path.join(PUB, p);
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) {
    res.writeHead(404); return res.end('nope');
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const page = await browser.newPage();
  page.on('pageerror', (e) => console.log('    [pageerror] ' + e.message));
  await page.route('**/*', (r) =>
    r.request().url().startsWith('http://localhost:' + PORT) ? r.continue() : r.abort());

  const load = async (turns) => {
    await page.goto('http://localhost:' + PORT + '/loop', { waitUntil: 'domcontentloaded' });
    await page.evaluate((t) => {
      // A fresh conversation each time: the panel persists state per project.
      try { localStorage.removeItem('cygenix_assistant_state::default'); } catch (e) {}
      window.CygenixAssistant._state().messages = [];
      window.CygenixAssistant._state().trail = [];
      window.CygenixAssistant._state().calls = 0;
      window.CygenixAssistant._state().lastCall = null;
      window.CygenixAssistant._state().repeatGuard = null;
      window.CygenixAssistant._state().status = 'idle';
      window.__turns = t;
    }, turns);
  };
  const settle = (want) => page.waitForFunction(
    (w) => w.indexOf(window.CygenixAssistant._state().status) !== -1,
    want || ['idle', 'confirm', 'stopped', 'error'], { timeout: 15000 });
  const state = () => page.evaluate(() => JSON.parse(JSON.stringify({
    status: window.CygenixAssistant._state().status,
    trail: window.CygenixAssistant._state().trail.map((t) => ({ title: t.title, error: !!t.error })),
    messages: window.CygenixAssistant._state().messages.length,
    last: (function () {
      var m = window.CygenixAssistant._state().messages;
      var lastMsg = m[m.length - 1];
      if (!lastMsg) return null;
      return typeof lastMsg.content === 'string' ? lastMsg.content
        : lastMsg.content.map(function (b) { return b.type + (b.text ? ':' + b.text : ''); }).join(',');
    })(),
  })));

  console.log('The assistant loop, driven end to end in a real browser\n');

  /* ── 1. The whole shape: read, choose, type, click, wait ─────────────────
     A safe control (Search) is used here so nothing pauses; the confirmation
     path gets its own scenario below. */
  await load([
    { tool: 'read_page' },
    { tool: 'type', label: 'Filter jobs', input: { text: 'nightly' } },
    { tool: 'choose', label: 'env', input: { option: 'Production' } },
    { tool: 'click', label: 'Search jobs' },
    { text: 'I filtered the list and searched.' },
  ]);
  await page.evaluate(() => window.CygenixAssistant.ask('filter the jobs and search'));
  await settle(['idle', 'error', 'stopped']);
  let s = await state();

  check('a run of five turns ends idle, not in error', s.status === 'idle', JSON.stringify(s));
  check('the field was actually filled in',
    (await page.evaluate(() => document.getElementById('filter').value)) === 'nightly');
  check('the dropdown was actually changed',
    (await page.evaluate(() => document.getElementById('env').value)) === 'Production');
  check('the button was actually pressed',
    (await page.evaluate(() => window.__ran)).join(',') === 'search');
  check('a safe control did not stop to ask', s.trail.every((t) => !t.error), JSON.stringify(s.trail));
  check('the trail names each step in the user\'s language',
    s.trail.map((t) => t.title).join(' | ') ===
      'Reading page | Typed into: Filter jobs | Chose: Production — env | Clicked: Search jobs',
    s.trail.map((t) => t.title).join(' | '));
  check('and the assistant\'s closing message is kept',
    /I filtered the list and searched\./.test(s.last || ''), s.last);

  /* ── 2. The one that matters: a confirmation actually holds the act ────── */
  await load([{ tool: 'read_page' }, { tool: 'click', label: 'Run job' }, { text: 'Started.' }]);
  await page.evaluate(() => window.CygenixAssistant.ask('run the job'));
  await settle(['confirm', 'idle', 'stopped', 'error']);
  s = await state();
  check('a consequential click parks for approval instead of running',
    s.status === 'confirm', JSON.stringify(s));
  check('AND THE BUTTON HAS NOT BEEN PRESSED',
    (await page.evaluate(() => window.__ran)).length === 0,
    'this is the assertion the whole confirmation design exists for');
  check('the dialog asks in the assistant\'s own words',
    /Assistant wants to click "Run job"\. Proceed\?/.test(
      await page.evaluate(() => document.querySelector('.cyga-confirm h4').textContent)),
    await page.evaluate(() => document.querySelector('.cyga-confirm h4').textContent));
  check('and says why it is asking',
    /not one of the controls that may be pressed without asking/.test(
      await page.evaluate(() => document.querySelector('.cyga-confirm pre').textContent)));

  await page.click('#cygaApprove');
  await settle(['idle', 'stopped', 'error']);
  check('approving runs it', (await page.evaluate(() => window.__ran)).join(',') === 'run');
  check('and the run continues to its end', (await state()).status === 'idle');

  /* Declining must leave the page untouched and tell the model not to retry. */
  await load([{ tool: 'read_page' }, { tool: 'click', label: 'Run job' }, { text: 'Understood.' }]);
  await page.evaluate(() => window.CygenixAssistant.ask('run the job'));
  await settle(['confirm']);
  await page.click('#cygaReject');
  await settle(['idle', 'stopped', 'error']);
  check('declining leaves the button unpressed',
    (await page.evaluate(() => window.__ran)).length === 0);
  check('and the trail names WHAT was skipped, not just which tool would have done it',
    (await state()).trail.some((t) => t.error && t.title === 'Skipped: Run job'),
    JSON.stringify((await state()).trail));

  /* ── 3. wait_for_change, in the loop, against a real reaction ──────────── */
  await load([
    { tool: 'read_page' },
    { tool: 'click', label: 'Search jobs' },
    { tool: 'wait_for_change', input: { description: 'the job starts', timeout_ms: 2000 } },
    { text: 'Done.' },
  ]);
  await page.evaluate(() => {
    document.getElementById('search').addEventListener('click', function () {
      setTimeout(function () { document.getElementById('out').innerHTML = '<h2>Results</h2>'; }, 150);
    });
    window.CygenixAssistant.ask('search and wait');
  });
  await settle(['idle', 'stopped', 'error']);
  s = await state();
  check('a wait detects the page reacting and rewrites its own trail row',
    s.trail.some((t) => /^Detected: /.test(t.title)), JSON.stringify(s.trail));
  check('and names what it saw',
    s.trail.some((t) => /Detected: a new section appeared: "Results"/.test(t.title)),
    JSON.stringify(s.trail));

  await load([
    { tool: 'wait_for_change', input: { description: 'something that never happens', timeout_ms: 600 } },
    { text: 'Nothing happened.' },
  ]);
  await page.evaluate(() => window.CygenixAssistant.ask('wait'));
  await settle(['idle', 'stopped', 'error']);
  s = await state();
  check('a timeout is not an error — the row says Timeout and the run carries on',
    s.status === 'idle' && s.trail.some((t) => t.title === 'Timeout' && !t.error),
    JSON.stringify(s.trail));

  /* ── 4. The brakes ──────────────────────────────────────────────────────── */
  await load(Array.from({ length: 20 }, () => ({ tool: 'read_page' })));
  await page.evaluate(() => window.CygenixAssistant.ask('read the page over and over'));
  await settle(['stopped', 'idle', 'error']);
  s = await state();
  check('an identical call twice in a row trips the loop brake',
    s.status === 'stopped' && s.trail.some((t) => /Detected a loop/.test(t.title)),
    JSON.stringify(s.trail));

  // Alternating scroll and read is not a repeat, so this reaches the budget.
  await load(Array.from({ length: 30 }, (_, i) =>
    (i % 2 ? { tool: 'scroll', input: { direction: i % 4 ? 'down' : 'up' } } : { tool: 'read_page' })));
  await page.evaluate(() => window.CygenixAssistant.ask('keep going'));
  await settle(['stopped', 'idle', 'error']);
  s = await state();
  check('fifteen tool calls is the budget, and the sixteenth stops the run',
    s.status === 'stopped' && s.trail.some((t) => /exceeded 15 tool calls/.test(t.title)),
    JSON.stringify(s.trail.map((t) => t.title)));
  const valid = await page.evaluate(() => {
    const m = window.CygenixAssistant._state().messages;
    const uses = [], results = [];
    m.forEach((msg) => (Array.isArray(msg.content) ? msg.content : []).forEach((b) => {
      if (b.type === 'tool_use') uses.push(b.id);
      if (b.type === 'tool_result') results.push(b.tool_use_id);
    }));
    return uses.every((id) => results.indexOf(id) !== -1);
  });
  check('and every tool call still got an answer, so the next turn is not rejected',
    valid === true,
    'an assistant message with an unanswered tool_use kills the conversation for good');

  /* ── 5. The assistant does not read itself ──────────────────────────────── */
  await load([{ tool: 'read_page' }, { text: 'ok' }]);
  await page.evaluate(() => { window.CygenixAssistant.open(); window.CygenixAssistant.ask('what is here'); });
  await settle(['idle', 'stopped', 'error']);
  const seen = await page.evaluate(() =>
    window.CygenixPageReader.lastRead().ids.map((i) => window.CygenixPageReader.lastRead().entries[i].label));
  check('with the panel open, its own controls are not in the read',
    !seen.some((l) => /^(Send|New|Ask Cygenix|Close assistant|New conversation)$/.test(l)),
    seen.join(' | '));
  check('but the page\'s own controls still are',
    seen.indexOf('Run job') !== -1 && seen.indexOf('Search jobs') !== -1, seen.join(' | '));

  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
