/* ============================================================================
   cygenix-fullscreen.js — one full-screen control, shared by every map
   ----------------------------------------------------------------------------
   The Schema map and the Data map each grew their own copy of the same
   Fullscreen API dance, and the six other views had none. This is the single
   implementation they all attach to.

     CygenixFullscreen.attach({
       button,                  // the ⤢ button
       target,                  // element, or () => element for a switching view
       onChange(isFull, target),// re-measure + re-render, on the next frame
       adopt,                   // overlays to carry in (see below)
       visible,                 // () => is this view the one on screen? (F key)
       label: { on, off },      // on = offers full screen, off = offers exit
     })  →  { enter, exit, toggle, isFull, target, detach }

   Three things this handles that a bare requestFullscreen() call does not:

   1. Esc. Leaving full screen never goes through the click handler, so button
      state is read from document.fullscreenElement on the browser's own
      change event, never from a local flag.
   2. Overlays. While an element is full screen, everything parented to <body>
      is painted underneath it — tooltips, legends and popovers simply vanish.
      Anything named in `adopt` is moved into the target on the way in and put
      back exactly where it was on the way out.
   3. Refusal. iOS Safari rejects requestFullscreen on anything that is not a
      video, and older browsers do not have it at all. Both fall back to a
      fixed-position pseudo-full-screen with the same public behaviour: same
      button state, same onChange, same Esc.
   ========================================================================== */
window.CygenixFullscreen = (function () {
'use strict';

var PSEUDO_CLASS = 'cyg-pseudo-fs';
var TARGET_CLASS = 'cyg-fs-target';
var DOC_CLASS    = 'cyg-fs-on';
var DEFAULT_LABEL = { on: '⤢ Full screen', off: '⤡ Exit full screen' };

var handles = [];
var lastNative = null;      // what was full screen before the current change
var entering = null;        // the handle that asked; it owns the transition
var wired = false;
var scrollLock = null;

function fsElement(){ return document.fullscreenElement || document.webkitFullscreenElement || null; }

/* Defaults, injected ahead of the page's own stylesheet so a page rule for a
   particular target — .sm-canvas-wrap:fullscreen and .da-panel:fullscreen
   already exist, with their own backgrounds — still wins. */
var CSS = [
  '.' + TARGET_CLASS + ':fullscreen,.' + TARGET_CLASS + ':-webkit-full-screen{',
  '  width:100vw;height:100vh;max-height:100vh;border-radius:0;margin:0;overflow:auto;background:var(--bg2)}',
  /* Doubled class: this stylesheet is injected ahead of the page's own so a
     page rule for a real :fullscreen target still wins, but the element being
     pinned is usually position:relative in that same page stylesheet, and a
     single class would lose to it on source order. */
  '.' + PSEUDO_CLASS + '.' + PSEUDO_CLASS + '{position:fixed;inset:0;z-index:10050;width:100vw;height:100vh;',
  '  max-height:100vh;margin:0;border-radius:0;overflow:auto;background:var(--bg2)}',
  /* The consent banner is fixed at z-index 9999 and would sit over a
     pseudo-full-screen map; while the real API is in use it is painted under
     the full-screen layer anyway, so hide it either way. */
  '.' + DOC_CLASS + ' #cc-banner,.' + DOC_CLASS + ' #cc-reopen{display:none !important}',
  /* An adopted overlay keeps working where it lands. The rail is the one that
     needs placing: it belongs beside the plot, not stacked under it. */
  /* A containing block for the adopted overlays. The pseudo target is already
     position:fixed, which is one — do not restate position here or it wins on
     source order and un-pins it. */
  '.' + TARGET_CLASS + ':fullscreen,.' + TARGET_CLASS + ':-webkit-full-screen{position:relative}',
  '.' + TARGET_CLASS + ':fullscreen > .cyg-fs-adopted.cdm-rail,',
  '.' + PSEUDO_CLASS + ' > .cyg-fs-adopted.cdm-rail{position:absolute;top:14px;right:16px;width:250px;z-index:4}',
].join('\n');

function injectCSS(){
  if (document.getElementById('cyg-fs-styles')) return;
  var s = document.createElement('style');
  s.id = 'cyg-fs-styles';
  s.textContent = CSS;
  if (document.head.firstChild) document.head.insertBefore(s, document.head.firstChild);
  else document.head.appendChild(s);
}

/* ---------- button state ------------------------------------------------- */
function syncButton(h){
  if (!h.button) return;
  var on = isFull(h);
  h.button.textContent = on ? h.label.off : h.label.on;
  h.button.classList.toggle('on', on);
  h.button.setAttribute('aria-pressed', String(on));
}
function isFull(h){
  if (h.pseudo) return true;
  var el = fsElement();
  return !!(el && h.target && el === h.target);
}

/* ---------- overlays ----------------------------------------------------- */
function resolveAdopt(h){
  var raw = typeof h.adopt === 'function' ? h.adopt() : h.adopt;
  var list = [];
  (raw || []).forEach(function (item) {
    if (!item) return;
    if (typeof item === 'string') {
      [].slice.call(document.querySelectorAll(item)).forEach(function (el) { list.push(el); });
    } else list.push(item);
  });
  return list;
}
function adopt(h){
  h.adopted = [];
  if (!h.target) return;
  resolveAdopt(h).forEach(function (el) {
    if (!el.parentNode || el === h.target || h.target.contains(el)) return;
    h.adopted.push({ el: el, parent: el.parentNode, next: el.nextSibling });
    el.classList.add('cyg-fs-adopted');
    h.target.appendChild(el);
  });
}
function release(h){
  h.adopted.forEach(function (rec) {
    rec.el.classList.remove('cyg-fs-adopted');
    if (rec.next && rec.next.parentNode === rec.parent) rec.parent.insertBefore(rec.el, rec.next);
    else rec.parent.appendChild(rec.el);
  });
  h.adopted = [];
}

/* ---------- re-measure --------------------------------------------------- */
function observe(h){
  if (h.ro || typeof ResizeObserver !== 'function' || !h.target) return;
  /* A tablet turned on its side while full screen changes the box without any
     fullscreenchange event of its own. */
  var first = true;
  h.ro = new ResizeObserver(function () {
    if (first) { first = false; return; }
    fire(h, true);
  });
  h.ro.observe(h.target);
}
function unobserve(h){
  if (!h.ro) return;
  try { h.ro.disconnect(); } catch (e) {}
  h.ro = null;
}
function fire(h, on){
  var t = h.target;
  var run = function () { try { h.onChange(on, t); } catch (e) {} };
  if (typeof requestAnimationFrame === 'function') requestAnimationFrame(run);
  else run();
}

/* ---------- entering and leaving ----------------------------------------- */
function settle(h, on){
  if (on) {
    adopt(h);
    observe(h);
    document.documentElement.classList.add(DOC_CLASS);
  } else {
    release(h);
    unobserve(h);
    if (!fsElement() && !handles.some(function (x) { return x.pseudo; })) {
      document.documentElement.classList.remove(DOC_CLASS);
    }
    if (h.target) h.target.classList.remove(TARGET_CLASS);
  }
  syncButton(h);
  fire(h, on);
}

function pseudoEnter(h){
  if (h.pseudo) return;
  h.pseudo = true;
  h.target.classList.add(PSEUDO_CLASS);
  if (scrollLock === null) {
    scrollLock = document.body.style.overflow || '';
    document.body.style.overflow = 'hidden';
  }
  settle(h, true);
}
function pseudoExit(h){
  if (!h.pseudo) return;
  h.pseudo = false;
  h.target.classList.remove(PSEUDO_CLASS);
  if (!handles.some(function (x) { return x.pseudo; })) {
    document.body.style.overflow = scrollLock || '';
    scrollLock = null;
  }
  settle(h, false);
}

function enter(h){
  var el = typeof h.getTarget === 'function' ? h.getTarget() : null;
  if (!el) return;
  var current = fsElement();
  if (current && current !== el) { exitAny(); }
  h.target = el;
  entering = h;
  el.classList.add(TARGET_CLASS);
  var req = el.requestFullscreen || el.webkitRequestFullscreen;
  if (!req) { pseudoEnter(h); return; }
  var r;
  try { r = req.call(el); } catch (e) { pseudoEnter(h); return; }
  /* Safari returns undefined rather than a promise, so guard the catch. */
  if (r && r.catch) r.catch(function () { pseudoEnter(h); });
}
function exitAny(){
  var x = document.exitFullscreen || document.webkitExitFullscreen;
  if (x) { try { x.call(document); } catch (e) {} }
}
function leave(h){
  if (h.pseudo) { pseudoExit(h); return; }
  exitAny();
}
function toggle(h){ isFull(h) ? leave(h) : enter(h); }

/* ---------- the browser's own change event ------------------------------- */
function onNativeChange(){
  var now = fsElement();
  var subject = now || lastNative;
  lastNative = now;
  handles.forEach(syncButton);
  /* Two controls can name the same element — the one that asked owns the
     transition, so its adopt list and onChange are the ones that run. */
  var h = (entering && entering.target === subject) ? entering
        : subject && handles.filter(function (x) { return x.target === subject; })[0];
  if (!now) entering = null;
  if (h) settle(h, !!now && now === h.target);
}

function onKeyDown(ev){
  if (ev.metaKey || ev.ctrlKey || ev.altKey) return;
  var el = ev.target;
  var typing = el && (/^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName) || el.isContentEditable);
  if (typing) return;
  if (ev.key === 'Escape') {
    var pseudo = handles.filter(function (h) { return h.pseudo; })[0];
    if (pseudo) { ev.preventDefault(); pseudoExit(pseudo); }
    return;
  }
  if (ev.key.toLowerCase() !== 'f') return;
  /* Only the view on screen answers to F. A handle with no `visible` test
     answers only while it is itself full screen. */
  var h = handles.filter(function (x) { return isFull(x); })[0]
       || handles.filter(function (x) { return x.visible && x.visible(); })[0];
  if (!h) return;
  ev.preventDefault();
  toggle(h);
}

function wire(){
  if (wired) return;
  wired = true;
  document.addEventListener('fullscreenchange', onNativeChange);
  document.addEventListener('webkitfullscreenchange', onNativeChange);
  document.addEventListener('keydown', onKeyDown);
}

/* ---------- public ------------------------------------------------------- */
function attach(opts){
  opts = opts || {};
  injectCSS();
  wire();
  var h = {
    button: opts.button || null,
    getTarget: typeof opts.target === 'function' ? opts.target : function () { return opts.target; },
    onChange: typeof opts.onChange === 'function' ? opts.onChange : function () {},
    adopt: opts.adopt || [],
    visible: typeof opts.visible === 'function' ? opts.visible : null,
    label: { on: (opts.label && opts.label.on) || DEFAULT_LABEL.on,
             off: (opts.label && opts.label.off) || DEFAULT_LABEL.off },
    target: null, pseudo: false, ro: null, adopted: [],
  };
  handles.push(h);
  var api = {
    enter: function () { enter(h); },
    exit: function () { leave(h); },
    toggle: function () { toggle(h); },
    isFull: function () { return isFull(h); },
    get target(){ return h.target; },
    detach: function () {
      if (isFull(h)) leave(h);
      unobserve(h);
      if (h.button && h.onClick) h.button.removeEventListener('click', h.onClick);
      var i = handles.indexOf(h);
      if (i >= 0) handles.splice(i, 1);   /* in place: __handles is that array */
    },
  };
  if (h.button) {
    h.onClick = function (ev) { ev.preventDefault(); toggle(h); };
    h.button.addEventListener('click', h.onClick);
    if (!h.button.getAttribute('type')) h.button.setAttribute('type', 'button');
    syncButton(h);
  }
  return api;
}

/* Is anything full screen right now? Views that pause work while hidden ask
   this before assuming they own the viewport. */
function active(){
  var h = handles.filter(isFull)[0];
  return h ? h.target : null;
}

return {
  attach: attach,
  active: active,
  isSupported: function () {
    var el = document.documentElement;
    return !!(el.requestFullscreen || el.webkitRequestFullscreen);
  },
  __handles: handles,
  PSEUDO_CLASS: PSEUDO_CLASS,
  TARGET_CLASS: TARGET_CLASS,
};
})();
