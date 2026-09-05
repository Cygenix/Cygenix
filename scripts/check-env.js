#!/usr/bin/env node
/* ============================================================================
   check-env.js — refuse to build a deployment that cannot save anyone's data.
   ----------------------------------------------------------------------------
   WHY

   CYGENIX_DATA_FN_KEY was absent from the Netlify deployment for roughly three
   weeks. The build succeeded. The site deployed. Every page loaded. And every
   single call to /api/data returned 503, so nothing anyone did was kept.

   A missing environment variable that breaks all persistence should not be
   able to reach production QUIETLY. This runs first in the build command and
   says so, in the build log, in the words of the thing that has to be set.

   WHY IT WARNS RATHER THAN REFUSES (changed 5 Sep 2026)

   The first version of this script exited non-zero and failed the build. It
   then blocked every production deploy for two days: the variable was set in
   Netlify, but not in a scope the BUILD could see (a variable scoped only to
   Functions is invisible here), so a correctly configured site could not
   ship anything, while the owner received a "Deploy failed" email per push.

   That is the wrong trade. Since #173 the RUNTIME is no longer quiet about a
   missing key — data-proxy logs it at error level, ?action=health reports
   no-fn-key, and the sync banner tells every user their work is not being
   saved — so the build gate was belt-and-braces, and its false positive was
   "no deploys at all". So: warn loudly, always; refuse only when the operator
   asks for the hard gate with CYGENIX_ENFORCE_ENV=true.

   WHAT IS AND IS NOT REQUIRED

   Only variables with NO working fallback are required. This distinction is
   the whole design:

     CYGENIX_DATA_FN_KEY   required. There is no default and cannot be one —
                           an embedded default is exactly how the last host key
                           ended up in the repository.

     CYGENIX_DATA_API_BASE optional. netlify/functions/data-proxy.js falls back
                           to the production hostname as a literal, and that
                           literal is correct. Requiring this would have failed
                           the very deploy that carried the fix, in an
                           environment where it is almost certainly unset.

   A variable is checked for PRESENCE, never for content. This script prints no
   value, no fragment of a value and no length — a build log is not a private
   place, and "the key ends in ...a4f" is still a disclosure.
   ========================================================================== */
'use strict';

/* Each entry: the variable, what breaks without it, and where to set it. The
   message is written for whoever is staring at a red build at 19:00 and has
   not read this file. */
const REQUIRED = [
  {
    name: 'CYGENIX_DATA_FN_KEY',
    breaks: 'Every call to /api/data returns 503. No project, job, connection '
          + 'or setting can be saved or loaded by anyone.',
    where: 'Netlify → Site configuration → Environment variables. The value is '
         + 'the Azure Function App host key (Portal → Function App → App keys). '
         + 'SCOPE MATTERS: this check runs during the BUILD, so the variable must '
         + 'be scoped to "All scopes" or at least include "Builds". A variable '
         + 'scoped only to "Functions" is correct for the proxy at runtime and '
         + 'invisible here — the build will refuse even though the site would work. '
         + 'If the variable IS set and you are reading this, that is almost '
         + 'certainly why.',
  },
];

/* Present-or-defaulted. Reported, never fatal. */
const OPTIONAL = [
  {
    name: 'CYGENIX_DATA_API_BASE',
    fallback: 'the production Function App hostname, hard-coded in '
            + 'netlify/functions/data-proxy.js',
  },
];

function main() {
  // A local `npm test` or a contributor's checkout has none of this set and
  // does not need it — the check is about what gets DEPLOYED. Netlify sets
  // NETLIFY=true in its build image.
  const isDeploy = process.env.NETLIFY === 'true' || process.env.CYGENIX_ENFORCE_ENV === 'true';
  // The hard gate is opt-in. See the header for why it is not the default.
  const enforce = process.env.CYGENIX_ENFORCE_ENV === 'true';

  const missing = REQUIRED.filter((v) => !String(process.env[v.name] || '').trim());

  OPTIONAL.forEach((v) => {
    if (!String(process.env[v.name] || '').trim()) {
      console.log('[check-env] ' + v.name + ' is not set — using ' + v.fallback + '.');
    } else {
      console.log('[check-env] ' + v.name + ' is set.');
    }
  });

  if (!missing.length) {
    console.log('[check-env] All required environment variables are present.');
    return 0;
  }

  const lines = missing.map((v) =>
    '\n  ' + v.name + '\n'
    + '    Without it: ' + v.breaks + '\n'
    + '    Set it in:  ' + v.where);

  if (!isDeploy) {
    // Local builds are not the place to enforce this, but silence would make
    // the check useless right up until it blocks a deploy nobody expected.
    console.log('[check-env] Not a deploy build — the following would FAIL a deploy:'
      + lines.join('') + '\n');
    return 0;
  }

  if (!enforce) {
    // Loud, and in the build log, and then let the deploy through. The site
    // will tell its users at runtime; this tells the operator now.
    console.error('\n[check-env] WARNING — deploying anyway.\n');
    console.error('This deployment is missing configuration without which the site '
      + 'deploys and then cannot save anything (every /api/data call returns 503, '
      + 'and users see the "not being saved" banner):'
      + lines.join('') + '\n');
    console.error('Set the variable(s) above and redeploy. To make this a hard '
      + 'failure instead of a warning, set CYGENIX_ENFORCE_ENV=true.\n');
    return 0;
  }

  console.error('\n[check-env] REFUSING TO BUILD (CYGENIX_ENFORCE_ENV=true).\n');
  console.error('This deployment is missing configuration without which the site '
    + 'deploys successfully and then silently fails to save anything:'
    + lines.join('') + '\n');
  console.error('Set the variable(s) above and redeploy. Nothing else in the '
    + 'build is wrong.\n');
  return 1;
}

process.exit(main());
