/* ============================================================================
   cygenix-enrichment.js — Data Enrichment engine
   ----------------------------------------------------------------------------
   Takes thin records — an email address, a company name — and completes them
   from reference data. This module owns every decision the page renders:
   anchor detection, the field catalogue, the natural-language rule compiler,
   normalisation and inference, waterfall reconciliation with conflicts and
   confidence, the staged-proposal store, and the apply / rollback SQL.

   The design principle inherited from Cleansing, enforced here: NOTHING is
   written to any database by this module or its page. A run stages
   proposals; the operator reviews; Apply generates UPDATE + rollback
   scripts and registers them as jobs for the Jobs module to execute.

   Honesty rules (spec §7.2), enforced in code, not copy:
   - phone numbers, email status, company registration numbers and revenue
     are VERIFY-ONLY: provider-attested or nothing, never inferred
   - inferred values are stamped origin:'inferred', capped at 0.80
     confidence, and can never clear an "accept all verified" bulk action
   - a row with no usable anchor lands in Unenrichable, never silently
     skipped
   - the compiled rule JSON — not the English — is what executes; anything
     the compiler cannot compile confidently comes back as a QUESTION,
     not a guess

   v1 judgement is a deterministic engine (same input + ruleset + seed →
   same proposals). The Claude normalise/match/reconcile loop of spec §7.1
   plugs in behind the same candidate/proposal shapes when the API layer is
   configured; the shapes here match its response schema on purpose.
   ========================================================================== */
(function (root, factory) {
  var api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root && typeof root === 'object') root.CygenixEnrichment = api;
})(typeof window !== 'undefined' ? window : this, function () {
'use strict';

var STORE_KEY = 'cygenix_enrichment_v1';
var INFERENCE_CAP = 0.80;
var PROPOSAL_CAP = 5000;

/* ── field catalogue (spec §5, contact + company) ───────────────────────── */
var CATALOGUE = [
  { attr: 'full_name',            group: 'contact', type: 'text', inferable: false },
  { attr: 'first_name',           group: 'contact', type: 'text', inferable: true,  note: 'split from full name' },
  { attr: 'last_name',            group: 'contact', type: 'text', inferable: true,  note: 'split from full name' },
  { attr: 'job_title',            group: 'contact', type: 'text', inferable: false },
  { attr: 'job_title_normalised', group: 'contact', type: 'text', inferable: true,  note: '"Hd of Fin." → "Head of Finance"' },
  { attr: 'seniority',            group: 'contact', type: 'enum', inferable: true,  note: 'IC / Manager / Director / VP / C-level' },
  { attr: 'department',           group: 'contact', type: 'enum', inferable: true,  note: 'Finance, IT, Ops, Sales, HR, Legal, Other' },
  { attr: 'email_status',         group: 'contact', type: 'enum', verifyOnly: true, note: 'valid / catch-all / invalid — provider-attested or nothing' },
  { attr: 'phone_direct',         group: 'contact', type: 'e164', verifyOnly: true },
  { attr: 'phone_mobile',         group: 'contact', type: 'e164', verifyOnly: true },
  { attr: 'linkedin_url',         group: 'contact', type: 'url',  inferable: false },
  { attr: 'company_name_legal',   group: 'company', type: 'text', inferable: false },
  { attr: 'company_domain',       group: 'company', type: 'text', inferable: true,  note: 'derived from a corporate email' },
  { attr: 'company_number',       group: 'company', type: 'text', verifyOnly: true, note: 'registry or nothing' },
  { attr: 'annual_revenue',       group: 'company', type: 'decimal', verifyOnly: true, note: 'never a bare number — carries currency + fiscal year' },
  { attr: 'headcount',            group: 'company', type: 'int',  inferable: false },
  { attr: 'headcount_band',       group: 'company', type: 'enum', inferable: true,  note: 'banded from a point estimate' },
  { attr: 'industry',             group: 'company', type: 'text', inferable: false, note: 'provider or SIC-derived' },
  { attr: 'sic_code',             group: 'company', type: 'text', verifyOnly: true },
  { attr: 'hq_country',           group: 'company', type: 'text', inferable: true,  note: 'ISO code; inferable from a country TLD only' },
  { attr: 'hq_city',              group: 'company', type: 'text', inferable: false },
  { attr: 'company_status',       group: 'company', type: 'enum', verifyOnly: true, note: 'active / dissolved / in administration' },
];
function attrOf(name) {
  return CATALOGUE.filter(function (c) { return c.attr === name; })[0] || null;
}

/* ── providers (spec §6). Only the internal reference table executes in v1;
   external connectors register through Integrations and are skipped —
   visibly — until configured. ───────────────────────────────────────────── */
var PROVIDERS = [
  { id: 'internal_ref', label: 'Internal reference table', kind: 'internal',
    desc: 'a clean table in your own estate — free, most trusted, runs first', costPerLookup: 0 },
  { id: 'companies_house', label: 'Companies House API', kind: 'external',
    desc: 'legal name · company no. · status · filings-derived revenue', costPerLookup: 0 },
  { id: 'contact_provider_a', label: 'B2B contact provider', kind: 'external',
    desc: 'titles · direct dials · verified emails', costPerLookup: 0.008 },
  { id: 'phone_verify', label: 'Phone verification', kind: 'external',
    desc: 'line type · verified flag', costPerLookup: 0.004 },
  { id: 'email_verify', label: 'Email verification', kind: 'external',
    desc: 'catch-all / invalid detection', costPerLookup: 0.002 },
];

/* ── table picker (spec §4 step 1) ───────────────────────────────────────
   The source table is chosen from the live catalogue, not typed from
   memory. Which table a fragment means is a decision, so the ranking and
   the default-schema resolution live here and are tested; the page only
   draws the menu.
   Tables are {schema, table, full}. ─────────────────────────────────────── */
function enRankTables(tables, query, limit) {
  var list = tables || [], cap = limit || 250;
  var q = String(query == null ? '' : query).trim().toLowerCase();
  if (!q) return list.slice(0, cap);
  var dot = q.indexOf('.');
  var qSchema = dot > -1 ? q.slice(0, dot) : null;
  var bare = dot > -1 ? q.slice(dot + 1) : q;
  var hits = [];
  list.forEach(function (t, i) {
    var full = String(t.full || (t.schema + '.' + t.table)).toLowerCase();
    var name = String(t.table || '').toLowerCase();
    var schema = String(t.schema || '').toLowerCase();
    var schemaOk = !qSchema || schema.indexOf(qSchema) === 0;
    var score = -1;
    if (full.indexOf(q) === 0) score = 0;                    /* dbo.crm → dbo.crm_contacts */
    else if (schemaOk && bare && name.indexOf(bare) === 0) score = 1;   /* crm → dbo.crm_contacts */
    else if (schemaOk && bare && name.indexOf(bare) > 0) score = 2;     /* contacts → dbo.crm_contacts */
    else if (!qSchema && full.indexOf(q) > 0) score = 3;
    if (score >= 0) hits.push({ t: t, score: score, i: i });
  });
  /* stable within a score band: the caller's order is already schema, name */
  hits.sort(function (a, b) { return a.score - b.score || a.i - b.i; });
  return hits.slice(0, cap).map(function (h) { return h.t; });
}

/* What a typed fragment resolves to once the operator stops typing. A bare
   name takes the default schema — unless the catalogue says it lives in
   exactly one other schema, in which case guessing "dbo" would be wrong. */
function enResolveTable(raw, tables, defaultSchema) {
  var dflt = defaultSchema || 'dbo';
  var v = String(raw == null ? '' : raw).trim().replace(/[[\]"`]/g, '');
  if (!v) return '';
  var list = tables || [];
  var lower = v.toLowerCase();
  if (v.indexOf('.') > -1) {
    /* echo the catalogue's own casing when it knows the table */
    var exact = list.filter(function (t) {
      return String(t.full || (t.schema + '.' + t.table)).toLowerCase() === lower; })[0];
    return exact ? (exact.full || (exact.schema + '.' + exact.table)) : v;
  }
  var byName = list.filter(function (t) { return String(t.table).toLowerCase() === lower; });
  if (byName.length === 1) return byName[0].full || (byName[0].schema + '.' + byName[0].table);
  var inDefault = byName.filter(function (t) {
    return String(t.schema).toLowerCase() === dflt.toLowerCase(); })[0];
  if (inDefault) return inDefault.full || (inDefault.schema + '.' + inDefault.table);
  return dflt + '.' + v;
}

/* Inline autofill: only ever EXTENDS what was typed. Completing "crm" to
   "dbo.crm_contacts" would rewrite text the operator did not type, so a
   fragment that is not a true prefix gets a highlighted menu row instead. */
function enInlineCompletion(typed, tables) {
  var v = String(typed == null ? '' : typed);
  if (!v.trim()) return null;
  var best = enRankTables(tables, v, 1)[0];
  if (!best) return null;
  var full = best.full || (best.schema + '.' + best.table);
  if (full.length <= v.length) return null;
  return full.toLowerCase().indexOf(v.toLowerCase()) === 0 ? full : null;
}

/* ── anchors (spec §3): the identifiers that find a record in the outside
   world. Detected from column names + null rates; operator-confirmable. ── */
function enDetectAnchors(columns) {
  var cols = columns || [];
  var find = function (re) {
    return cols.filter(function (c) { return re.test(c.name); })
      .sort(function (a, b) { return (a.nullPct || 0) - (b.nullPct || 0); })[0] || null;
  };
  var out = [];
  var email = find(/mail/i);
  var company = find(/(company|account|org(anisation)?|business).*name|^(company|account|customer)$/i);
  var phone = find(/^(tel|phone|mob)/i);
  var mk = function (anchor, col, derived) {
    var cov = derived ? Math.max(0, Math.round(100 - (email.nullPct || 0)) - 4)
                      : Math.round(100 - (col.nullPct || 0));
    out.push({ anchor: anchor, column: derived ? 'derived from ' + email.name : col.name,
      coverage: cov, enabled: cov >= 60, derived: !!derived });
  };
  if (email) mk('Email', email);
  if (company) mk('Company name', company);
  if (email) mk('Domain', email, true);
  if (phone) mk('Phone', phone);
  return out;
}

/* ── normalisation + inference (deterministic v1 of spec §7.1/§7.2) ─────── */
var COMPANY_SUFFIX_RE = /\b(limited|ltd\.?|plc|llp|inc\.?|gmbh|s\.?a\.?|co\.?)\b/gi;
function enNormCompany(name) {
  return String(name || '').toLowerCase()
    .replace(COMPANY_SUFFIX_RE, '')
    .replace(/[^a-z0-9 ]+/g, ' ')
    .replace(/\s+/g, ' ').trim();
}
var TITLE_EXPANSIONS = {
  hd: 'Head', fin: 'Finance', mgr: 'Manager', dir: 'Director', snr: 'Senior',
  ops: 'Operations', eng: 'Engineering', mktg: 'Marketing', acct: 'Accounts',
  admin: 'Administration', asst: 'Assistant', exec: 'Executive', tech: 'Technology',
};
function enNormTitle(title) {
  var words = String(title || '').replace(/\./g, '').split(/\s+/).filter(Boolean);
  return words.map(function (w) {
    var key = w.toLowerCase();
    if (TITLE_EXPANSIONS[key]) return TITLE_EXPANSIONS[key];
    if (/^(of|and|the|for|to|in)$/i.test(w)) return key;
    if (/^(it|hr|ceo|cfo|cto|coo|vp|qa|ai)$/i.test(w)) return w.toUpperCase();
    return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
  }).join(' ');
}
function enDomainOf(email) {
  var m = /@([^@\s>]+)$/.exec(String(email || '').trim());
  return m ? m[1].toLowerCase() : null;
}
var FREE_EMAIL_RE = /^(gmail|googlemail|hotmail|outlook|yahoo|icloud|aol|proton(mail)?|live|msn)\./i;
function enSeniorityOf(title) {
  var t = String(title || '').toLowerCase();
  if (!t.trim()) return null;
  if (/chief|c[ef]o\b|cto|coo|cio|founder|president|managing director/.test(t)) return 'C-level';
  if (/\bvp\b|vice president/.test(t)) return 'VP';
  if (/director|head of/.test(t)) return 'Director';
  if (/manager|lead\b|supervisor/.test(t)) return 'Manager';
  return 'IC';
}
function enDepartmentOf(title) {
  var t = String(title || '').toLowerCase();
  if (!t.trim()) return null;
  if (/financ|account|treasur|payroll|audit/.test(t)) return 'Finance';
  if (/\bit\b|technolog|software|developer|engineer|data|infra|security/.test(t)) return 'IT';
  if (/operat|logistic|supply|facilit|production/.test(t)) return 'Ops';
  if (/sales|commercial|business development|revenue/.test(t)) return 'Sales';
  if (/\bhr\b|people|talent|recruit/.test(t)) return 'HR';
  if (/legal|counsel|compliance/.test(t)) return 'Legal';
  return 'Other';
}
var TLD_COUNTRY = { 'co.uk': 'GB', 'uk': 'GB', 'de': 'DE', 'fr': 'FR', 'ie': 'IE',
  'nl': 'NL', 'es': 'ES', 'it': 'IT', 'au': 'AU', 'nz': 'NZ', 'ca': 'CA', 'ch': 'CH' };
function enCountryOfDomain(domain) {
  var d = String(domain || '').toLowerCase();
  var m = /\.([a-z]{2}\.[a-z]{2}|[a-z]{2,})$/.exec(d);
  if (!m) return null;
  return TLD_COUNTRY[m[1]] || null;            /* .com / .io / .org: honestly unknown */
}

/* ── natural-language rules → JSON (spec §7.3, deterministic grammar) ───
   The compiled JSON — not the English — executes. A sentence the compiler
   does not recognise comes back in questions[], never guessed at. ──────── */
var COUNTRY_WORDS = { uk: ['GB', 'UK'], britain: ['GB', 'UK'], 'united kingdom': ['GB', 'UK'],
  ireland: ['IE'], germany: ['DE'], france: ['FR'], us: ['US'], usa: ['US'], america: ['US'] };
function enCompileRules(nl, ctx) {
  ctx = ctx || {};
  var out = { filters: [], transforms: [], provider_precedence: {} };
  var questions = [], compiled = [];
  var sentences = String(nl || '').split(/[.;\n]+/).map(function (s) { return s.trim(); })
    .filter(Boolean);
  sentences.forEach(function (s) {
    /* one sentence can carry several rules ("Only enrich UK accounts that
       are missing a phone number" is a country filter AND a null filter) —
       every pattern checks independently, hit only decides question-vs-not */
    var low = s.toLowerCase(), hit = false;

    Object.keys(COUNTRY_WORDS).forEach(function (w) {
      if (new RegExp('\\b' + w + '\\b').test(low) && /only|restrict|just/.test(low)) {
        out.filters.push({ column: ctx.countryColumn || 'hq_country', op: 'in', value: COUNTRY_WORDS[w] });
        hit = true;
      }
    });
    if (/\b(missing|no|without)\b[a-z ]*\b(phone|number|tel)/.test(low)) {
      out.filters.push({ column: ctx.phoneColumn || 'phone', op: 'is_null' });
      hit = true;
    }
    if (/dissolved|struck off|closed/.test(low) && /(don'?t|do not|never|exclude|skip|touch)/.test(low)) {
      out.filters.push({ column: ctx.statusColumn || 'company_status', op: 'neq', value: 'dissolved' });
      hit = true;
    }
    if (/revenue/.test(low) && /(usd|dollars?)/.test(low) && /(gbp|pounds?|sterling)/.test(low)) {
      var basis = (/fy\s?(\d{2})/.exec(low) || [])[1];
      out.transforms.push({ field: 'annual_revenue', op: 'fx_convert', to: 'GBP',
        rate_basis: basis ? 'FY' + basis + '_avg' : 'spot', annotate: true });
      hit = true;
    }
    if (/prefer|precedence|first/.test(low) && /companies house/.test(low)) {
      var field = /legal name/.test(low) ? 'company_name_legal'
        : /revenue/.test(low) ? 'annual_revenue' : 'company_name_legal';
      out.provider_precedence[field] = ['companies_house', '*'];
      hit = true;
    }
    if (hit) compiled.push(s);
    else {
      questions.push('I could not compile "' + s + '" into a deterministic rule — rephrase it, or drop it and it will not apply.');
    }
  });
  return { ruleJson: out, questions: questions, compiled: compiled };
}

function ident(dialect, name) {
  return dialect === 'postgres'
    ? '"' + String(name).replace(/"/g, '""') + '"'
    : '[' + String(name).replace(/\]/g, ']]') + ']';
}
function lit(v) { return "'" + String(v == null ? '' : v).replace(/'/g, "''") + "'"; }

/* the compiled filters as a WHERE predicate — how "rows in scope" is counted
   and how the run's batch SELECT is restricted. Read-only by construction. */
function enFiltersToSql(ruleJson, dialect) {
  var parts = ((ruleJson && ruleJson.filters) || []).map(function (f) {
    var c = ident(dialect, f.column);
    if (f.op === 'is_null') return '(' + c + ' IS NULL OR LTRIM(RTRIM(CAST(' + c + " AS VARCHAR(64)))) = '')";
    if (f.op === 'in') return c + ' IN (' + f.value.map(lit).join(', ') + ')';
    if (f.op === 'neq') return '(' + c + ' IS NULL OR ' + c + ' <> ' + lit(f.value) + ')';
    if (f.op === 'eq') return c + ' = ' + lit(f.value);
    throw new Error('unknown filter op ' + f.op);
  });
  return parts.length ? parts.join(' AND ') : '1=1';
}

/* ── reconciliation (deterministic v1 of the §7.1 loop) ──────────────────
   candidates: [{ provider, providerOrder, matchScore, attrs: {attr: value},
                  detail? }] — one per provider per row.
   Returns proposals + conflicts, in the same shape the Claude response
   schema uses, so the API-backed adjudicator drops in later. ───────────── */
var PROVIDER_TRUST = { internal_ref: 0.98, companies_house: 0.99,
  contact_provider_a: 0.90, phone_verify: 0.92, email_verify: 0.92 };

function enReconcile(row, mappings, candidates, opts) {
  opts = opts || {};
  var floor = opts.floor != null ? opts.floor : 0.75;
  var precedence = (opts.ruleJson && opts.ruleJson.provider_precedence) || {};
  var proposals = [], conflicts = [];
  /* the title context grows as the row is enriched: seniority inferred from
     a title THIS run proposed, not only from what the row already held */
  var ctxTitle = (row.values && (row.values[opts.titleColumn] || row.values.job_title)) || null;

  mappings.forEach(function (m) {
    var meta = attrOf(m.attr);
    if (!meta) return;
    var current = row.values ? row.values[m.targetColumn] : null;
    var isNullish = current == null || String(current).trim() === '';

    /* fill policy gate before anything is even considered */
    if (m.policy === 'if-null' && !isNullish) return;

    /* gather candidate values for this attribute */
    var cands = [];
    candidates.forEach(function (c) {
      var v = c.attrs && c.attrs[m.attr];
      if (v == null || String(v).trim() === '') return;
      cands.push({ provider: c.provider, value: v,
        confidence: Math.round(100 * (c.matchScore || 0.8) * (PROVIDER_TRUST[c.provider] || 0.85)) / 100,
        order: c.providerOrder || 99, detail: c.detail || null });
    });

    var chosen = null;
    if (cands.length) {
      var pref = precedence[m.attr];
      if (pref) {
        for (var i = 0; i < pref.length && !chosen; i++) {
          if (pref[i] === '*') break;
          chosen = cands.filter(function (c) { return c.provider === pref[i]; })[0] || null;
        }
      }
      if (!chosen) chosen = cands.slice().sort(function (a, b) {
        return a.order - b.order || b.confidence - a.confidence; })[0];
      /* disagreement — normalised comparison so LTD vs Limited is not a conflict */
      var norm = function (v) { return meta.attr.indexOf('company_name') === 0
        ? enNormCompany(v) : String(v).trim().toLowerCase(); };
      cands.forEach(function (c) {
        if (c === chosen) return;
        if (norm(c.value) !== norm(chosen.value)) {
          conflicts.push({ rowKey: row.key, name: m.attr, chosen: chosen.value,
            rejected: [{ value: c.value, source: c.provider }],
            reason: pref
              ? 'Rule set pins ' + m.attr + ' to ' + chosen.provider + ' ahead of any other provider.'
              : chosen.provider + ' sits above ' + c.provider + ' in the waterfall; both values are recorded.' });
        }
      });
    }

    if (chosen) {
      var proposedVal = meta.attr === 'job_title_normalised' || meta.attr === 'job_title'
        ? enNormTitle(chosen.value) : chosen.value;
      if (meta.attr === 'job_title' || meta.attr === 'job_title_normalised') ctxTitle = proposedVal;
      proposals.push({
        rowKey: row.key, targetColumn: m.targetColumn, attr: m.attr,
        current: current == null ? null : current,
        proposed: proposedVal,
        origin: chosen.provider === 'internal_ref' ? 'internal' : 'provider',
        source: chosen.provider, confidence: chosen.confidence,
        reason: chosen.detail
          || (chosen.provider === 'internal_ref'
            ? 'Found in your own clean reference table — cheapest and most trusted, so it runs first.'
            : 'Returned by ' + chosen.provider + ' for this row\'s anchor.'),
        needsReview: chosen.confidence < floor,
      });
      return;
    }

    /* no provider value: inference, where honesty allows it */
    if (meta.verifyOnly) {
      proposals.push({ rowKey: row.key, targetColumn: m.targetColumn, attr: m.attr,
        current: current == null ? null : current, proposed: null,
        origin: 'none', source: 'no verified value', confidence: 0,
        reason: m.attr + ' is verify-only — provider-attested or nothing. No provider returned a verifiable value, so nothing is proposed.',
        needsReview: false, nothingProposed: true });
      return;
    }
    if (opts.inferenceEnabled === false || !meta.inferable) return;

    var inferred = null, why = null, conf = 0;
    if (m.attr === 'seniority' && ctxTitle) {
      inferred = enSeniorityOf(ctxTitle); conf = 0.71;
      why = 'Derived from the job title "' + ctxTitle + '". No provider asserted a seniority field.';
    } else if (m.attr === 'department' && ctxTitle) {
      inferred = enDepartmentOf(ctxTitle); conf = 0.68;
      why = 'Derived from the job title "' + ctxTitle + '".';
    } else if (m.attr === 'job_title_normalised' && ctxTitle) {
      inferred = enNormTitle(ctxTitle); conf = 0.78;
      why = 'Normalised from the raw title "' + ctxTitle + '".';
    } else if (m.attr === 'company_domain' && row.anchors && row.anchors.email) {
      var d = enDomainOf(row.anchors.email);
      if (d && !FREE_EMAIL_RE.test(d)) { inferred = d; conf = 0.76;
        why = 'Derived from the corporate email domain.'; }
      else if (d) { why = 'Email domain ' + d + ' is a free provider — not a company domain.'; }
    } else if (m.attr === 'hq_country' && row.anchors && row.anchors.email) {
      var dom = enDomainOf(row.anchors.email);
      var cc = enCountryOfDomain(dom);
      if (cc) { inferred = cc; conf = 0.64;
        why = 'Inferred from the country TLD of ' + dom + '. A generic TLD would have produced nothing.'; }
    }
    if (inferred != null) {
      proposals.push({ rowKey: row.key, targetColumn: m.targetColumn, attr: m.attr,
        current: current == null ? null : current, proposed: inferred,
        origin: 'inferred', source: 'engine inference', confidence: Math.min(INFERENCE_CAP, conf),
        reason: why + ' AI-inferred: confidence capped at ' + INFERENCE_CAP
          + ' and excluded from "accept all verified".',
        needsReview: Math.min(INFERENCE_CAP, conf) < floor });
    }
  });
  return { proposals: proposals, conflicts: conflicts };
}

/* ── estimate (spec §4 step 4 / §12.9) ──────────────────────────────────── */
function enEstimate(rowCount, providerIds, fieldCount) {
  var lookups = 0, providerCost = 0;
  providerIds.forEach(function (id) {
    var p = PROVIDERS.filter(function (x) { return x.id === id; })[0];
    if (!p) return;
    lookups += rowCount;
    providerCost += rowCount * (p.costPerLookup || 0);
  });
  /* deterministic engine costs nothing; the Claude adjudication tier is
     estimated only when it is actually configured */
  return { lookups: lookups, providerCost: Math.round(providerCost * 100) / 100, llmCost: 0 };
}

/* ── apply + rollback SQL ────────────────────────────────────────────────
   Both scripts are STAGED artifacts: registered as jobs, never executed
   here. Shadow columns carry provenance into the target when mapped. ───── */
function enApplySql(proposals, opts) {
  var d = opts.dialect || 'sqlserver';
  var t = ident(d, opts.schema || 'dbo') + '.' + ident(d, opts.table);
  var key = ident(d, opts.keyColumn);
  var lines = ['-- Cygenix Data Enrichment · apply script · run ' + opts.runId,
    '-- ' + proposals.length + ' accepted proposal(s). Review before executing;',
    '-- the paired rollback script restores every pre-enrichment value.', ''];
  proposals.forEach(function (p) {
    var sets = [ident(d, p.targetColumn) + ' = ' + lit(p.proposed)];
    if (opts.shadow !== false) {
      sets.push(ident(d, p.targetColumn + '_source') + ' = ' + lit(p.source));
      sets.push(ident(d, p.targetColumn + '_confidence') + ' = ' + p.confidence);
      sets.push(ident(d, p.targetColumn + '_retrieved_at') + ' = ' + lit(opts.date || ''));
    }
    lines.push('UPDATE ' + t + ' SET ' + sets.join(', ')
      + ' WHERE ' + key + ' = ' + lit(p.rowKey) + ';');
  });
  return lines.join('\n');
}
function enRollbackSql(proposals, opts) {
  var d = opts.dialect || 'sqlserver';
  var t = ident(d, opts.schema || 'dbo') + '.' + ident(d, opts.table);
  var key = ident(d, opts.keyColumn);
  var lines = ['-- Cygenix Data Enrichment · rollback script · run ' + opts.runId,
    '-- restores every pre-enrichment value captured on the proposal.', ''];
  proposals.forEach(function (p) {
    lines.push('UPDATE ' + t + ' SET ' + ident(d, p.targetColumn) + ' = '
      + (p.current == null ? 'NULL' : lit(p.current))
      + ' WHERE ' + key + ' = ' + lit(p.rowKey) + ';');
  });
  return lines.join('\n');
}

/* ── store (spec §8, browser-local) ─────────────────────────────────────── */
function enNewStore(now) {
  return {
    v: 1, createdAt: now || 0,
    runs: [], proposals: [], rulesets: [], events: [],
    settings: { floor: 0.75, inferenceEnabled: true, retentionDays: 30,
      spendCapGBP: 75, disclosureAck: {} },
  };
}
function enLoad() {
  try {
    var raw = (typeof localStorage !== 'undefined') && localStorage.getItem(STORE_KEY);
    if (raw) return JSON.parse(raw);
  } catch (e) { /* fall through */ }
  return enNewStore(Date.now ? Date.now() : 0);
}
function enSave(store) {
  try { localStorage.setItem(STORE_KEY, JSON.stringify(store)); } catch (e) { /* quota */ }
}
function enEvent(store, ev, now) {
  store.events.push(Object.assign({ at: now || 0 }, ev));
  if (store.events.length > 400) store.events.splice(0, store.events.length - 400);
}

function enSaveRuleset(store, name, nlText, ruleJson, user, now) {
  var prior = store.rulesets.filter(function (r) { return r.name === name; });
  var rs = { rulesetId: 'rs_' + (now || 0).toString(36) + '_' + store.rulesets.length,
    name: name, version: prior.length + 1, nl_source_text: nlText, rule_json: ruleJson,
    createdBy: user || '', createdAt: now || 0 };
  store.rulesets.push(rs);
  return rs;
}

function enRecordRun(store, run, now) {
  var r = Object.assign({ runId: 'enr_' + (now || 0).toString(36) + '_' + store.runs.length,
    status: 'complete', startedAt: now || 0 }, run);
  store.runs.push(r);
  enEvent(store, { type: 'run.recorded', runId: r.runId, rows: r.rowCount }, now);
  return r;
}

function enStageProposals(store, runId, proposals, now) {
  proposals.forEach(function (p, i) {
    store.proposals.push(Object.assign({
      proposalId: runId + '_p' + i, runId: runId,
      verdict: 'pending', verdictBy: null, verdictAt: null, editedValue: null,
      verdictTrail: [],
    }, p));
  });
  if (store.proposals.length > PROPOSAL_CAP) {
    store.proposals.splice(0, store.proposals.length - PROPOSAL_CAP);
  }
}

/* proposals are never deleted by a verdict or an apply — the trail IS the
   audit record */
function enVerdict(store, proposalId, verdict, editedValue, user, now) {
  var p = store.proposals.filter(function (x) { return x.proposalId === proposalId; })[0];
  if (!p) return null;
  p.verdictTrail.push({ verdict: verdict, by: user || '', at: now || 0 });
  p.verdict = verdict;
  p.verdictBy = user || '';
  p.verdictAt = now || 0;
  if (verdict === 'edited') p.editedValue = editedValue;
  return p;
}

/* right to erasure (spec §9): purge one row's proposals, leave a tombstone */
function enErase(store, runId, rowKey, user, now) {
  var before = store.proposals.length;
  store.proposals = store.proposals.filter(function (p) {
    return !(p.runId === runId && String(p.rowKey) === String(rowKey));
  });
  var removed = before - store.proposals.length;
  enEvent(store, { type: 'row.erased', runId: runId, rowKey: String(rowKey),
    removed: removed, by: user || '' }, now);
  return removed;
}

return {
  STORE_KEY: STORE_KEY,
  CATALOGUE: CATALOGUE,
  PROVIDERS: PROVIDERS,
  INFERENCE_CAP: INFERENCE_CAP,
  attrOf: attrOf,

  enRankTables: enRankTables,
  enResolveTable: enResolveTable,
  enInlineCompletion: enInlineCompletion,

  enDetectAnchors: enDetectAnchors,
  enNormCompany: enNormCompany,
  enNormTitle: enNormTitle,
  enDomainOf: enDomainOf,
  enSeniorityOf: enSeniorityOf,
  enDepartmentOf: enDepartmentOf,
  enCountryOfDomain: enCountryOfDomain,

  enCompileRules: enCompileRules,
  enFiltersToSql: enFiltersToSql,
  enReconcile: enReconcile,
  enEstimate: enEstimate,
  enApplySql: enApplySql,
  enRollbackSql: enRollbackSql,

  enNewStore: enNewStore,
  enLoad: enLoad,
  enSave: enSave,
  enEvent: enEvent,
  enSaveRuleset: enSaveRuleset,
  enRecordRun: enRecordRun,
  enStageProposals: enStageProposals,
  enVerdict: enVerdict,
  enErase: enErase,
};
});
