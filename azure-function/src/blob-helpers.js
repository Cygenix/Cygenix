/* blob-helpers.js — validation shared by the blob-list, blob-download and
 * blob-upload actions in index.js.
 *
 * These three endpoints proxy calls to a CLIENT-OWNED Azure storage account
 * using a SAS the user pasted in. That makes them the one place in the
 * Function App where a caller chooses the host we fetch, so the validation
 * here is a security boundary, not a convenience:
 *
 *   * the hostname is checked by exact SUFFIX, never substring — '.blob.'
 *     as a substring matches x.blob.core.windows.net.attacker.com and turns
 *     this into an SSRF gadget;
 *   * the URL must be a CONTAINER SAS (exactly one path segment), so a blob
 *     name can never be smuggled in through the SAS itself;
 *   * blob names may contain '/' (Azure virtual directories) but never '..',
 *     backslashes or a leading slash, all of which reposition the target.
 *
 * Extracted so both the shipped endpoints and the tests run the same code.
 */
'use strict';

const AZURE_BLOB_SUFFIX = '.blob.core.windows.net';

// Validate a container SAS URL. Returns { ok, hostname, pathname, sasToken }
// or { ok:false, error } with a message suitable for returning to the caller.
function validateContainerSas(sasUrl) {
  const raw = String(sasUrl || '').trim();
  if (!raw) return { ok: false, error: 'sasUrl required' };

  let parsed;
  try { parsed = new URL(raw); }
  catch { return { ok: false, error: 'sasUrl is not a valid URL' }; }

  if (parsed.protocol !== 'https:') {
    return { ok: false, error: 'sasUrl must use https' };
  }
  if (!parsed.hostname.toLowerCase().endsWith(AZURE_BLOB_SUFFIX)) {
    return { ok: false, error: 'sasUrl hostname does not look like Azure Blob Storage' };
  }
  const segs = parsed.pathname.split('/').filter(Boolean);
  if (segs.length === 0) return { ok: false, error: 'sasUrl is missing the container name' };
  if (segs.length > 1)   return { ok: false, error: 'sasUrl must be a container SAS (one path segment)' };

  const sasToken = parsed.search.startsWith('?') ? parsed.search.slice(1) : parsed.search;
  if (!sasToken) return { ok: false, error: 'sasUrl has no SAS token' };
  if (!/[?&]sig=/.test('?' + sasToken)) {
    return { ok: false, error: 'SAS token is missing the signature (sig=) parameter' };
  }

  return { ok: true, hostname: parsed.hostname, pathname: parsed.pathname, sasToken };
}

// Validate a blob name and return the URL-encoded form to append to the
// container path. Virtual-directory slashes survive; every other segment is
// encoded, so spaces and unicode in filenames round-trip correctly.
function validateBlobName(blobName) {
  const name = String(blobName || '').trim();
  if (!name) return { ok: false, error: 'blobName required' };
  if (name.includes('..') || name.includes('\\')) {
    return { ok: false, error: 'blobName contains forbidden characters' };
  }
  if (name.startsWith('/')) {
    return { ok: false, error: 'blobName must be relative to the container' };
  }
  if (name.endsWith('/')) {
    return { ok: false, error: 'blobName must not end with a slash' };
  }
  // Azure's own limit. Beyond it the PUT fails upstream anyway; failing here
  // gives the caller a message that says what is wrong.
  if (name.length > 1024) {
    return { ok: false, error: 'blobName is longer than Azure allows (1024 characters)' };
  }
  const encoded = name.split('/').map(encodeURIComponent).join('/');
  return { ok: true, name, encoded };
}

// Build the full blob URL for a validated SAS + blob name.
function blobUrl(sas, encodedBlob) {
  return 'https://' + sas.hostname + sas.pathname + '/' + encodedBlob + '?' + sas.sasToken;
}

// The permissions a SAS grants, read off its `sp=` parameter.
//
// Azure's letters: r read, w write, l list, c create, d delete, a add.
// Upload needs create OR write: `c` alone is enough for a new blob, `w` is
// required to replace one. Knowing this up front lets the UI say which
// permission is missing instead of surfacing a bare 403 from Azure.
function sasPermissions(sasToken) {
  let sp = '';
  try {
    sp = new URLSearchParams(String(sasToken || '')).get('sp') || '';
  } catch { sp = ''; }
  const has = (ch) => sp.toLowerCase().includes(ch);
  return {
    raw: sp,
    read:   has('r'),
    write:  has('w'),
    list:   has('l'),
    create: has('c'),
    delete: has('d'),
    add:    has('a'),
    // `sp` is absent on user-delegation SAS variants and on some older
    // tokens. Unknown is not the same as denied — we let the attempt run and
    // report whatever Azure says rather than blocking on our own guess.
    unknown: !sp,
  };
}

function canUpload(perms)    { return !!(perms.unknown || perms.create || perms.write); }
function canOverwrite(perms) { return !!(perms.unknown || perms.write); }
function canDelete(perms)    { return !!(perms.unknown || perms.delete); }
// Azure has no rename. It is a server-side copy followed by a delete of the
// original, so it needs BOTH permissions — and the copy has to come first, so
// a failure leaves a duplicate rather than nothing.
function canRename(perms)    { return canUpload(perms) && canDelete(perms); }

// Turn an Azure Blob REST error into something a person can act on. Azure
// returns XML with an <Message> element and an x-ms-error-code header; the
// bare status alone ("403") tells the user nothing about which permission
// their SAS is missing.
function describeBlobError(status, bodyText, errorCode, opts) {
  const o = opts || {};
  const m = String(bodyText || '').match(/<Message[^>]*>([\s\S]*?)<\/Message>/);
  const detail = m ? m[1].trim().split('\n')[0] : '';
  const code = String(errorCode || '').trim();

  if (status === 403) {
    if (o.delete) return 'Azure refused the delete (403). The SAS needs Delete permission (sp=…d…) and must not be expired.';
    return o.write
      ? 'Azure refused the write (403). The SAS needs Create and Write permission (sp=…cw…) and must not be expired.'
      : 'Azure refused the request (403). The SAS may be expired, or lack the required permission.';
  }
  if (status === 404) {
    if (o.delete) return 'That blob no longer exists (404) — someone may have removed it already.';
    return o.write
      ? 'Container not found (404). Check the container name in the SAS URL.'
      : 'Not found (404). The blob or container does not exist.';
  }
  if (status === 409 || status === 412 || /BlobAlreadyExists/i.test(code)) {
    return o.rename
      ? 'A blob with that name already exists. Pick a different name.'
      : 'A blob with that name already exists. Tick "Overwrite existing files" to replace it.';
  }
  if (status === 416 || /InvalidRange/i.test(code)) {
    return 'Azure rejected the range of the upload. Try the file again.';
  }
  return (detail || ('HTTP ' + status)) + (code ? ' (' + code + ')' : '');
}

module.exports = {
  AZURE_BLOB_SUFFIX,
  validateContainerSas,
  validateBlobName,
  blobUrl,
  sasPermissions,
  canUpload,
  canOverwrite,
  canDelete,
  canRename,
  describeBlobError,
};
