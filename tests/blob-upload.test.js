// Tests for Azure Blob upload — the proxy endpoint's validation helpers and
// the Blob Source tab's upload wiring.
//
// These endpoints are the one family in the Function App where the CALLER
// chooses the host we fetch, and upload is the one that WRITES into a client's
// storage account. Two properties matter more than the feature working:
//
//   * the SAS hostname check must be an exact suffix, so the proxy cannot be
//     pointed at an attacker's host (SSRF);
//   * a non-overwriting upload must be enforced by a precondition on the PUT,
//     not by a check-then-write, because a file destroyed in someone else's
//     storage account cannot be recovered.
'use strict';

const fs = require('fs');
const path = require('path');

const B = require(path.join(__dirname, '..', 'azure-function', 'src', 'blob-helpers.js'));

let pass = 0, fail = 0;
const check = (label, ok, extra) => {
  if (ok) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + (extra ? '  → ' + extra : '')); }
};

const SAS = 'https://acct.blob.core.windows.net/cont?sv=2024-01-01&sr=c&sp=racwl&sig=abc123';

// ── SAS validation ──────────────────────────────────────────────────────────
{
  const good = B.validateContainerSas(SAS);
  check('a container SAS validates and yields its parts',
    good.ok && good.hostname === 'acct.blob.core.windows.net'
    && good.pathname === '/cont' && /sig=abc123/.test(good.sasToken));

  // The SSRF case. '.blob.' as a SUBSTRING matches this hostname, which is
  // why the check is an exact suffix.
  const evil = B.validateContainerSas('https://x.blob.core.windows.net.attacker.com/cont?sig=a');
  check('a lookalike hostname is refused, not fetched',
    !evil.ok && /Azure Blob Storage/.test(evil.error));

  check('http is refused', !B.validateContainerSas('http://acct.blob.core.windows.net/cont?sig=a').ok);
  check('a blob-level SAS is refused — only container SAS',
    !B.validateContainerSas('https://acct.blob.core.windows.net/cont/file.csv?sig=a').ok);
  check('a URL with no container is refused',
    !B.validateContainerSas('https://acct.blob.core.windows.net/?sig=a').ok);
  check('a SAS with no signature is refused',
    !B.validateContainerSas('https://acct.blob.core.windows.net/cont?sv=2024').ok);
  check('an empty sasUrl is refused', !B.validateContainerSas('').ok);
  check('garbage is refused rather than thrown', !B.validateContainerSas('not a url').ok);
}

// ── Blob name validation ────────────────────────────────────────────────────
{
  const ok1 = B.validateBlobName('Staging/KY/report 2026.csv');
  check('virtual-directory slashes survive, spaces are encoded',
    ok1.ok && ok1.encoded === 'Staging/KY/report%202026.csv');

  const uni = B.validateBlobName('données/ünïcode.csv');
  check('unicode round-trips through encoding',
    uni.ok && decodeURIComponent(uni.encoded) === 'données/ünïcode.csv');

  check('path traversal is refused', !B.validateBlobName('../../etc/passwd').ok);
  check('backslashes are refused', !B.validateBlobName('a\\b.csv').ok);
  check('a leading slash is refused', !B.validateBlobName('/abs.csv').ok);
  check('a trailing slash is refused — that is a directory, not a file',
    !B.validateBlobName('folder/').ok);
  check('an empty name is refused', !B.validateBlobName('   ').ok);
  check('a name past Azure\'s 1024-character limit is refused',
    !B.validateBlobName('a'.repeat(1025)).ok);

  const sas = B.validateContainerSas(SAS);
  check('the built URL keeps container, blob and token in the right places',
    B.blobUrl(sas, ok1.encoded)
      === 'https://acct.blob.core.windows.net/cont/Staging/KY/report%202026.csv?'
        + 'sv=2024-01-01&sr=c&sp=racwl&sig=abc123');
}

// ── SAS permissions ─────────────────────────────────────────────────────────
{
  const rl   = B.sasPermissions('sv=2024&sp=rl&sig=x');
  const racwl= B.sasPermissions('sv=2024&sp=racwl&sig=x');
  const c    = B.sasPermissions('sv=2024&sp=rcl&sig=x');
  const none = B.sasPermissions('sv=2024&sig=x');

  check('a read-list SAS is read-only', rl.read && rl.list && !rl.write && !rl.create);
  check('and cannot upload', !B.canUpload(rl) && !B.canOverwrite(rl));
  check('racwl can upload and overwrite', B.canUpload(racwl) && B.canOverwrite(racwl));
  check('create-without-write can upload but not replace',
    B.canUpload(c) && !B.canOverwrite(c));
  check('an absent sp is unknown, not denied — Azure gets the final word',
    none.unknown && B.canUpload(none) && B.canOverwrite(none));
  check('permission letters are case-insensitive',
    B.canUpload(B.sasPermissions('sp=RACWL&sig=x')));
}

// ── Error translation ───────────────────────────────────────────────────────
{
  check('403 on a write names the permissions the SAS needs',
    /Create and Write/.test(B.describeBlobError(403, '', '', { write: true })));
  check('403 on a read does not talk about writing',
    !/Create and Write/.test(B.describeBlobError(403, '', '')));
  check('409 BlobAlreadyExists points at the Overwrite tickbox',
    /Overwrite existing files/.test(B.describeBlobError(409, '', 'BlobAlreadyExists', { write: true })));
  check('412 (the If-None-Match rejection) says the same thing',
    /Overwrite existing files/.test(B.describeBlobError(412, '', '', { write: true })));
  check('404 on a write points at the container name',
    /Container not found/.test(B.describeBlobError(404, '', '', { write: true })));
  check('an unrecognised status surfaces Azure\'s own message',
    /Authorization failed/.test(
      B.describeBlobError(400, '<Error><Message>Authorization failed\nRequestId:x</Message></Error>', '')));
}

// ── The endpoint, as shipped in index.js ────────────────────────────────────
{
  const idx = fs.readFileSync(path.join(__dirname, '..', 'azure-function', 'src', 'index.js'), 'utf8');
  const start = idx.indexOf("case 'blob-upload':");
  const body = start > -1 ? idx.slice(start, idx.indexOf("\n        default:", start)) : '';

  check('the blob-upload action exists', start > -1);
  check('it PUTs with the BlockBlob type header Azure requires',
    /'x-ms-blob-type': 'BlockBlob'/.test(body) && /method: 'PUT'/.test(body));

  // The core guarantee: not a check-then-write.
  check('a non-overwriting upload carries If-None-Match, so Azure enforces it',
    /if \(!overwrite\) putHeaders\['If-None-Match'\] = '\*';/.test(body));
  check('overwrite is opt-in — it is only true when the header says 1',
    /const overwrite = String\(req\.headers\.get\('x-blob-overwrite'\) \|\| ''\) === '1';/.test(body));

  check('the SAS is validated before any fetch',
    body.indexOf('validateContainerSas') < body.indexOf('fetch(targetUrl'));
  check('the blob name is validated too', /validateBlobName/.test(body));
  check('a SAS that cannot write is refused before the bytes are read',
    body.indexOf('canUpload(perms)') < body.indexOf('req.arrayBuffer()'));
  check('the 100MB cap is checked from the declared length and again from the body',
    /content-length/.test(body) && /arrayBuf\.byteLength > MAX_BYTES/.test(body));
  check('an empty body is refused', /Upload body is empty/.test(body));
  check('a name that is not valid percent-encoding is refused, not thrown',
    /not valid percent-encoding/.test(body));
  check('an already-exists answer is reported as 409, not as a server fault',
    /resp\.status === 409 \|\| resp\.status === 412\) \? 409/.test(body));

  // Preflight: the browser drops these headers unless the Function allows
  // them, and the symptom is an upload that silently never starts.
  check('the x-blob-* headers are on the CORS allow-list',
    /Access-Control-Allow-Headers[^\n]*x-blob-sas, x-blob-name, x-blob-overwrite/.test(idx));
  check('blob-upload is named in the unknown-action list',
    /blob-list, blob-download, blob-upload/.test(idx));

  // The refactor that put list/download/upload on one validator.
  check('list and download share the same validator, so they cannot drift',
    (idx.match(/validateContainerSas\(/g) || []).length >= 3);
}

// ── The page wiring ─────────────────────────────────────────────────────────
{
  const app  = fs.readFileSync(path.join(__dirname, '..', 'public', 'dashboard-app.js'), 'utf8');
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'dashboard.html'), 'utf8');

  check('the upload panel is in the blob browser', /id="blob-upload-panel"/.test(html));
  check('it takes several files at once', /id="blob-upload-input"[^>]*multiple/.test(html));
  check('there is a destination-folder field and an overwrite tickbox',
    /id="blob-upload-prefix"/.test(html) && /id="blob-upload-overwrite"/.test(html));
  check('the drop zone is reachable by keyboard, not only by mouse',
    /id="blob-upload-drop"[\s\S]{0,400}tabindex="0"/.test(html));
  check('overwrite defaults to off in the markup',
    !/id="blob-upload-overwrite"[^>]*checked/.test(html));
  check('the SAS help line now mentions the upload permissions',
    /add <code[^>]*>Create<\/code> \+ <code[^>]*>Write<\/code> to upload/.test(html));

  check('the client reads sp= to decide what a SAS can do',
    /function blobSourceSasPerms/.test(app) && /canUpload:\s*unknown \|\| has\('c'\) \|\| has\('w'\)/.test(app));
  check('an undeclared sp is treated as unknown, not as denied',
    /unknown \|\| has\('w'\)/.test(app));
  check('upload uses XHR, because fetch reports no upload progress',
    /new XMLHttpRequest\(\)/.test(app) && /xhr\.upload\.onprogress/.test(app));
  check('the SAS travels in a header, never the query string, so it stays out of logs',
    /setRequestHeader\('x-blob-sas', sasUrl\)/.test(app)
    && !/blob-upload\?[^']*sasUrl/.test(app));
  check('the blob name is percent-encoded for the latin-1 header',
    /setRequestHeader\('x-blob-name', encodeURIComponent\(target\)\)/.test(app));
  check('files upload one at a time — several 100MB buffers at once is an OOM',
    /for \(const item of pending\)\{[\s\S]{0,900}await blobUploadOne/.test(app));
  check('replacing existing files asks first, naming them',
    /confirm\('Overwrite '/.test(app) && /cannot be recovered by Cygenix/.test(app));
  check('a 409 is shown as skipped, not as an error',
    /r\.status === 409 \? 'skipped' : 'error'/.test(app));
  check('the disabled Upload button carries the reason it is disabled',
    /go\.disabled = !!reason;/.test(app) && /go\.title = reason/.test(app));
  check('destination folders cannot climb out of the container',
    /seg === '\.\.'/.test(app));
  check('the listing refreshes after a successful upload',
    /if \(done\) await blobSourceReloadFiles\(\);/.test(app));
  check('switching or closing a source drops the queue with it',
    (app.match(/BlobUploadState\.queue = \[\];/g) || []).length >= 2);
  check('the browser cannot be closed mid-upload',
    /if \(BlobUploadState\.running\)\{[\s\S]{0,200}wait for it to finish/.test(app));
  check('dragover is prevented, or the browser navigates away to the file',
    /on\(zone, 'dragover', \(e\) => \{\s*e\.preventDefault\(\);/.test(app));
  check('saved sources show whether they can be written to',
    /read-only<\/span>/.test(app) && /read \+ write<\/span>/.test(app));
  check('the upload handlers are exposed for the inline onclick attributes',
    /window\.blobUploadStart\s*=\s*blobUploadStart;/.test(app));
}

// ── Copy path / URL, and the shortcut into restore ──────────────────────────
{
  const app  = fs.readFileSync(path.join(__dirname, '..', 'public', 'dashboard-app.js'), 'utf8');

  // Run the shipped path helpers against a stub state, so the encoding under
  // test is the encoding that ships.
  const vm = require('vm');
  const slice = app.slice(app.indexOf('function blobSourceIsBackup'),
                          app.indexOf('// Copy text, wherever the browser allows it'));
  const sb = { String, encodeURIComponent };
  sb.BlobSourceState = { activeParsed: { accountName: 'lesece20269drow', containerName: 'backupcontainer' } };
  vm.createContext(sb);
  vm.runInContext(slice, sb);
  const rel = (n) => vm.runInContext('blobSourceRelativePath', sb)(n);
  const url = (n) => vm.runInContext('blobSourceBlobUrl', sb)(n);
  const isBak = (n) => vm.runInContext('blobSourceIsBackup', sb)(n);

  check('the relative path is the blob path, unencoded',
    rel('AdventureWorks2025 (2).bak') === 'AdventureWorks2025 (2).bak');
  check('a nested path keeps its slashes', rel('Staging/KY/foo.bak') === 'Staging/KY/foo.bak');
  check('a trailing slash is stripped', rel('folder/file.bak/') === 'folder/file.bak');

  check('the full URL is account + container + encoded path',
    url('AdventureWorks2025 (2).bak')
      === 'https://lesece20269drow.blob.core.windows.net/backupcontainer/AdventureWorks2025%20(2).bak');
  check('spaces encode to %20', /%20/.test(url('a b.bak')));
  check('virtual-directory slashes survive encoding',
    url('Staging/KY/foo.bak').endsWith('/backupcontainer/Staging/KY/foo.bak'));

  // encodeURI would leave these intact and truncate the URL at the query or
  // fragment boundary — a link to the wrong blob, or to none.
  check('a question mark in a filename is encoded, not left to start a query',
    url('what?.bak').includes('what%3F.bak'));
  check('a hash is encoded, not left to start a fragment',
    url('v#2.bak').includes('v%232.bak'));
  check('a plus sign is encoded', url('a+b.bak').includes('a%2Bb.bak'));

  check('no SAS token reaches either copied value',
    !url('x.bak').includes('sig=') && !url('x.bak').includes('?'));
  check('the helper refuses to invent a URL with no source open',
    (() => { const saved = sb.BlobSourceState.activeParsed;
             sb.BlobSourceState.activeParsed = null;
             const out = url('x.bak');
             sb.BlobSourceState.activeParsed = saved;
             return out === ''; })());

  check('.bak, .trn and .dif are restore sources',
    isBak('a.bak') && isBak('a.trn') && isBak('a.dif') && isBak('A.BAK'));
  check('a CSV is not', !isBak('a.csv') && !isBak('a.bak.csv'));

  // Wiring.
  check('every row offers both copy values, URL first',
    app.indexOf(">⧉ Copy URL<") > -1 && app.indexOf(">⧉ Path<") > -1
    && app.indexOf(">⧉ Copy URL<") < app.indexOf(">⧉ Path<"));
  check('the copy buttons sit in the same action cell as Download',
    /importBtn \+ restoreBtn \+ downloadBtn \+ copyBtns/.test(app));
  check('copy confirmation lands on the row that was clicked',
    /function blobSourceFlashCopied/.test(app) && /blobSourceCopyBlobPath\(this,/.test(app));
  check('the confirmation is transient and restores the original label',
    /btn\.innerHTML = btn\._copyLabel;/.test(app) && /}, 1400\);/.test(app));
  check('copy falls back for non-secure contexts',
    /document\.execCommand\('copy'\)/.test(app) && /navigator\.clipboard\.writeText/.test(app));

  check('the restore shortcut is offered for backups only',
    /blobSourceIsBackup\(b\.name\)\s*\?\s*'<button/.test(app));
  check('and is not gated on the 100MB cap — the server pulls the blob itself',
    !/tooBig[^\n]*restoreBtn|restoreBtn[^\n]*tooBig/.test(app));
  check('it switches tab, picks the Azure Blob URL radio and fills the path',
    /switchConnTab\('restore'\)/.test(app)
    && /input\[name="rst-mode"\]\[value="url"\]/.test(app)
    && /input\.value = url;/.test(app));
  check('it prefills only — never Inspect, never Run',
    (() => { const fn = app.slice(app.indexOf('function blobSourceUseInRestore'),
                                  app.indexOf('// What a SAS is allowed to do'));
             return !/rstInspect\(|rstRun\(|rstValidate\(/.test(fn); })());
  check('the handlers are exposed for the inline onclick attributes',
    /window\.blobSourceCopyBlobPath\s*=/.test(app) && /window\.blobSourceUseInRestore\s*=/.test(app));

  // An apostrophe in a filename used to break every button on its row: these
  // values sit in single-quoted JS strings inside an onclick attribute, and
  // encodeURIComponent does not escape "'".
  check('an apostrophe in a filename cannot break the row\'s onclick handlers',
    /encodeURIComponent\(b\.name\)\.replace\(\/'\/g, '%27'\)/.test(app));

  check('azcopy still carries the SAS — it has no other way to authenticate',
    /blobSourceBlobUrl\(cleanName\) \+ '\?' \+ parsed\.sasToken/.test(app));
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
