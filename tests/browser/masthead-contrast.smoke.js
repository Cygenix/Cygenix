/* tests/browser/masthead-contrast.smoke.js
 * ---------------------------------------------------------------------------
 * The masthead search field can actually be read.
 *
 * THE BUG THIS EXISTS FOR
 * The masthead sits on navy, so its search field was written for navy:
 * transparent fill, white text, a white-at-75% placeholder. Meanwhile
 * cygenix-console.css styles every form control for the light page body:
 *
 *     .form-input, input[type="text"], input[type="search"], ... {
 *       color: var(--color-text); background: var(--color-bg); }
 *
 * input[type="search"] scores one attribute plus one type. A bare
 * .cx-mh-search scores one class. The attribute selector wins, so the field
 * was quietly handed the PAGE's palette — a near-white fill. The placeholder
 * rule did NOT lose, because .cx-mh-search::placeholder outscores a bare
 * ::placeholder. White hint text, white box: 1.06:1. Not low contrast —
 * invisible. A user reported it as "others tell me there is a hint message in
 * that box"; they had never seen it.
 *
 * WHY THIS HAS TO BE A BROWSER TEST
 * Nothing was wrong with either stylesheet. Both rules were valid, both were
 * intended, and each one read correctly on its own. What was wrong was which
 * of them won, and that is decided by the cascade at paint time. Grepping the
 * source cannot tell you; only something that renders both files together and
 * reads getComputedStyle can. So this file measures the real contrast ratio of
 * what is actually painted, rather than asserting that a particular colour
 * string appears in a particular file.
 *
 * It also explains why the bug survived so long: the moment you TYPE, the text
 * is dark-on-light and looks perfectly fine. Only the hint was lost — so the
 * field looked healthy to anyone who tested it by using it.
 *
 * Not part of `npm test`: it needs a browser. Run it by hand:
 *   node tests/browser/masthead-contrast.smoke.js
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright-core');

const PUB = path.join(__dirname, '..', '..', 'public');
const PORT = Number(process.env.SMOKE_PORT || 8411);
const EXE = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + String(extra).slice(0, 300) : '')); }
};

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json',
  '.woff2': 'font/woff2' };

const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/') p = '/index.html';
  let f = path.join(PUB, p);
  if (!fs.existsSync(f) && fs.existsSync(f + '.html')) f += '.html';
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) {
    res.writeHead(404); return res.end('no');
  }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)] || 'application/octet-stream' });
  res.end(fs.readFileSync(f));
});

/* WCAG 2.x relative luminance and contrast, computed here rather than pulled
   in, because the whole point of this file is to not take a second opinion on
   what is on the screen. Colours arrive from getComputedStyle as rgb()/rgba()
   strings; a translucent one is composited over what is behind it first, which
   is exactly what the browser does when it paints. */
const rgba = (s) => {
  const n = (s.match(/[\d.]+/g) || []).map(Number);
  return { r: n[0] || 0, g: n[1] || 0, b: n[2] || 0, a: n.length > 3 ? n[3] : 1 };
};
const over = (fg, bg) => ({
  r: fg.a * fg.r + (1 - fg.a) * bg.r,
  g: fg.a * fg.g + (1 - fg.a) * bg.g,
  b: fg.a * fg.b + (1 - fg.a) * bg.b,
  a: 1,
});
const lum = (c) => {
  const ch = [c.r, c.g, c.b].map((v) => {
    const s = v / 255;
    return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * ch[0] + 0.7152 * ch[1] + 0.0722 * ch[2];
};
const ratio = (a, b) => {
  const [hi, lo] = [lum(a), lum(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
};
const r1 = (n) => Math.round(n * 10) / 10;

(async () => {
  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.route('**/*', (r) =>
    r.request().url().startsWith('http://localhost:' + PORT) ? r.continue() : r.abort());
  await page.addInitScript(() => {
    const exp = String(Date.now() + 3600e3);
    for (const s of [localStorage, sessionStorage]) {
      s.setItem('cygenix_token', 'smoke'); s.setItem('cygenix_expires', exp);
    }
    localStorage.setItem('cygenix_onboarded', '1');
    localStorage.setItem('cygenix_user', JSON.stringify({ email: 'you@example.test', name: 'You' }));
    localStorage.setItem('cygenix_tier', 'pro');
    localStorage.setItem('cygenix_cookie_consent', 'all');
    localStorage.setItem('acct-cygenix.ciamlogin.com-x', JSON.stringify({
      homeAccountId: 'x', environment: 'cygenix.ciamlogin.com', authorityType: 'MSSTS',
      username: 'you@example.test', localAccountId: 'x', tenantId: 'x' }));
  });

  console.log('Masthead search field — is the hint text actually readable?\n');

  await page.goto('http://localhost:' + PORT + '/dashboard', { waitUntil: 'domcontentloaded' });
  await page.waitForSelector('#cx-mh-search', { timeout: 15000 });
  await page.waitForTimeout(600);

  const painted = await page.evaluate(() => {
    const el = document.querySelector('#cx-mh-search');
    const cs = getComputedStyle(el);
    const ph = getComputedStyle(el, '::placeholder');
    const bar = getComputedStyle(document.querySelector('.cx-masthead'));
    const icon = document.querySelector('.cx-mh-ic');
    return {
      bar: bar.backgroundColor,
      field: cs.backgroundColor,
      text: cs.color,
      hint: ph.color,
      hintOpacity: ph.opacity,
      fontSize: parseFloat(cs.fontSize),
      placeholder: el.getAttribute('placeholder') || '',
      label: el.getAttribute('aria-label') || '',
      iconPainted: !!(icon && icon.getBoundingClientRect().width > 6),
      padLeft: parseFloat(cs.paddingLeft),
      iconRight: icon ? icon.getBoundingClientRect().right - el.getBoundingClientRect().left : 0,
      // Does the hint fit, or is it clipped at the end? Measured, not guessed.
      hintWidth: (() => {
        const c = document.createElement('canvas').getContext('2d');
        c.font = cs.fontWeight + ' ' + cs.fontSize + ' ' + cs.fontFamily;
        return c.measureText(el.getAttribute('placeholder') || '').width;
      })(),
      inner: el.clientWidth - parseFloat(cs.paddingLeft) - parseFloat(cs.paddingRight),
    };
  });

  /* ── 1. The field is not wearing the light page palette ────────────────── */
  const bar = rgba(painted.bar);
  const field = over(rgba(painted.field), bar);
  check('the field sits on the dark bar, not on the light page palette',
    lum(field) < 0.2, painted.field + ' over ' + painted.bar);

  /* ── 2. The hint, which is the thing that was invisible ────────────────── */
  const hint = over(rgba(painted.hint), field);
  const hintRatio = ratio(hint, field);
  check('THE PLACEHOLDER CLEARS WCAG AAA AGAINST WHAT IS BEHIND IT (7:1)',
    hintRatio >= 7, r1(hintRatio) + ':1  hint ' + painted.hint + ' on ' + painted.field);
  check('and it is not diluted by a browser default opacity',
    Number(painted.hintOpacity) >= 0.99, painted.hintOpacity);

  /* ── 3. Typed text too — it was fine by accident before, not by design ─── */
  const text = over(rgba(painted.text), field);
  const textRatio = ratio(text, field);
  check('typed text clears AAA as well, now by design rather than by accident',
    textRatio >= 7, r1(textRatio) + ':1  text ' + painted.text + ' on ' + painted.field);

  /* ── 4. The hint is only ever a hint ───────────────────────────────────── */
  // A placeholder disappears the moment you type, so it can never be the only
  // name a screen reader or a magnifier user has for the field.
  check('the field carries a real accessible name, not just a placeholder',
    painted.label.length > 0 && painted.label !== painted.placeholder,
    painted.label + ' / ' + painted.placeholder);
  check('a magnifier marks the field, so it announces itself without reading text',
    painted.iconPainted);
  check('and the text starts clear of that magnifier rather than under it',
    painted.padLeft >= painted.iconRight, 'pad ' + painted.padLeft + ' vs icon ends ' + r1(painted.iconRight));

  /* ── 5. It fits ────────────────────────────────────────────────────────── */
  check('the whole hint fits the field — a truncated hint is a hint half lost',
    painted.hintWidth <= painted.inner,
    r1(painted.hintWidth) + 'px of text in ' + r1(painted.inner) + 'px');
  check('nothing on this control is set below 12px',
    painted.fontSize >= 12, painted.fontSize + 'px');

  /* ── 6. Focus is still visible ─────────────────────────────────────────── */
  await page.focus('#cx-mh-search');
  await page.waitForTimeout(120);
  const focused = await page.evaluate(() => {
    const c = getComputedStyle(document.querySelector('#cx-mh-search'));
    return { outline: c.outlineColor, width: parseFloat(c.outlineWidth), bg: c.backgroundColor };
  });
  check('focus still draws a visible outline against the bar',
    focused.width >= 2 && ratio(over(rgba(focused.outline), bar), bar) >= 3,
    r1(ratio(over(rgba(focused.outline), bar), bar)) + ':1 at ' + focused.width + 'px');
  check('and the fill lifts on focus, so the field reads as active',
    focused.bg !== painted.field, focused.bg);

  console.log('\n    hint ' + r1(hintRatio) + ':1 · typed ' + r1(textRatio)
    + ':1 · field ' + painted.field + ' on bar ' + painted.bar);

  check('nothing threw along the way', errors.length === 0, errors.slice(0, 3).join(' | '));

  await browser.close();
  server.close();
  console.log('\n' + pass + '/' + (pass + fail) + ' checks passed');
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); server.close(); process.exit(1); });
