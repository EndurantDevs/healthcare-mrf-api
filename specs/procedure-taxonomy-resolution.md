# Procedure-to-Taxonomy Resolution

**Status:** Proposed
**Date:** 2026-06-26
**Scope:** `healthcare-mrf-api` and implementation-neutral API consumers
**Related:** `specs/in-network-provider-specialty-filter.md`, `specs/cms_claims_pricing_v1.md`, `docs/imports/clinical-reference.md`

## Problem

The platform can filter a plan's providers by NPPES/NUCC taxonomy once a caller
passes `specialty`, `classification`, or `taxonomy_codes`. What it cannot do
reliably today is answer this upstream question:

> Given a CPT/HCPCS/CDT code, or a plain-language care intent, which provider
> taxonomy filter should we use before recommending doctors?

This matters because negotiated MRF pricing proves only that a provider set has
a contracted rate for a billing code. It does not prove every listed NPI
actually performs or clinically owns that service. Office visit codes such as
`99213` and `99214` are the clearest failure mode: many specialists bill them,
but a member asking for flu care should not be sent to a vascular surgeon or
radiologist just because the plan has a negotiated office-visit price.

Current hardcoded logic exists in several places:

- `api/provider_specialty_filters.py` maps free-text specialties to NUCC code
  sets for plan provider filters.
- `api/ptg2_code_filters.py` maps broad CPT numeric ranges to inferred
  provider taxonomies for some PTG2 serving paths.
- Some API consumers keep small hand-written specialty and care-intent mappings,
  including client-side logic to enrich each NPI and filter after pricing.

Those lists should become fallbacks only. The durable solution is a derived,
auditable procedure-to-taxonomy evidence layer with explicit safety gates. The
resolver must not turn weak or biased utilization evidence into an invisible
hard provider exclusion.

## Source Findings

### Available public source

Use CMS Medicare Physician & Other Practitioners "by Provider and Service" as
the primary evidence feed. It is public, annual, and organized by:

- rendering NPI,
- HCPCS/CPT code,
- place of service,
- provider type,
- service/beneficiary/payment counts.

The existing `claims-pricing` importer already resolves this CMS catalog feed
and materializes the main rows in:

- `mrf.pricing_provider`
- `mrf.pricing_provider_procedure`
- `mrf.pricing_provider_procedure_location`
- `mrf.pricing_provider_procedure_cost_profile`
- `mrf.pricing_procedure_peer_stats`

The importer currently derives `specialty_key` from CMS `provider_type` for cost
profiles. That is useful evidence, but it is not yet the NUCC taxonomy filter we
need for MRF plan provider search. Any CMS-provider-type-to-NUCC relationship is
a bridge/curation problem and must be modeled as evidence or override metadata,
not inferred by string equality.

NPPES and NUCC are already imported and should be the identity layer:

- `mrf.npi_taxonomy` maps NPI to self-selected taxonomy code, including primary
  taxonomy.
- `mrf.nucc_taxonomy` maps taxonomy code to grouping, classification, and
  specialization.
- `mrf.pricing_provider_quality_feature` already materializes one selected
  taxonomy code and taxonomy classification per `(npi, year)`. Use it as the
  fast primary-taxonomy evidence surface when present, and fall back to direct
  `mrf.npi_taxonomy`/`mrf.nucc_taxonomy` joins when secondary taxonomy,
  mismatch analysis, or bootstrap runs need the full taxonomy set.

### Bias and coverage limits

CMS Physician & Other Practitioners is Medicare fee-for-service data. It is not
a representative commercial or pediatric population feed. It excludes Medicare
Advantage and younger commercial members, and source cells with fewer than 11
beneficiaries are suppressed before import. The importer maps suppressed `*` and
`NA` values to `NULL`, so missing minority-specialty evidence is not proof that
the specialty does not perform the service.

Consequences:

- high CMS service share can be high-confidence *Medicare* evidence while still
  being low-confidence for a young-skew or commercial-skew code;
- service-share thresholds reward high-volume adult specialties and can bury
  lower-volume but clinically correct specialties;
- broad office visit codes and mixed adult/pediatric codes need intent or
  curated rules before producing a member-facing provider filter;
- codes with thin or biased CMS evidence must trigger fallback/curation, not
  merely return "no mapping".

### What not to do

Do not load or depend on a full proprietary CPT-to-specialty dictionary unless
the deployment has explicit licensing. The existing clinical-reference docs
already state that complete public CPT/HCPCS/CDT procedure-to-clinical-area
mapping is not available from the public source set, and CPT/CDT descriptors
must not be treated as licensed reference imports.

Do not infer correctness from MRF negotiated pricing alone. Pricing is a
contract artifact, not a clinical competency signal.

Do not automatically use the top CMS provider types for every code without care
intent. For broad E/M codes, the CMS distribution will correctly show many
specialties. That means the code is ambiguous; it does not mean every specialty
is appropriate for every member question.

Do not emit a hard provider pre-filter from Medicare-only evidence unless a
representativeness gate passes or a reviewed override explicitly permits it.
False positives are bad, but false negatives are also clinically unsafe: a hard
taxonomy `EXISTS` predicate can silently drop a legitimate in-network provider
whose NPPES primary taxonomy is missing, stale, generic, or miscoded.

## Proposed Architecture

Add a derived resolver owned by `healthcare-mrf-api`:

1. Build evidence from existing claims-pricing rows.
2. Join observed NPIs to the existing primary-taxonomy surface
   (`pricing_provider_quality_feature`) and to full NPPES/NUCC taxonomy when
   needed.
3. Aggregate signal by procedure code, setting, and taxonomy. Keep CMS provider
   type as evidence, not as the row grain.
4. Publish a resolver API that returns both a recommended provider-filter mode
   and ranked evidence. The mode decides whether taxonomy is a hard filter, a
   ranking boost, or validation-only.
5. Let API consumers call this resolver instead of carrying private taxonomy
   lists.

This should be an additional finalize step of `claims-pricing` or a small
dependent importer named `procedure-taxonomy`. It should not re-download the
large CMS feed if the current claims-pricing tables already exist.

The current hardcoded code-range rules, such as orthopedic CPT ranges, should be
promoted into reviewed overrides and used as first-class curated evidence where
claims representativeness is weak. They should not remain invisible Python
branches that automatically hard-filter provider search.

## Data Model

### `mrf.procedure_taxonomy_signal`

One row per observed code/year/setting/taxonomy signal. This can be a
materialized table or a versioned materialized view. It must not duplicate the
full cost-profile grain; it is a lightweight taxonomy aggregate over existing
claims-pricing outputs.

Recommended columns:

- `code_system varchar not null`
- `code varchar not null`
- `procedure_code bigint null`
- `year int not null`
- `place_of_service varchar null`
- `setting_key varchar not null` (`facility`, `office`, `all`, etc.). Existing
  `pricing_provider_procedure_cost_profile` currently hardcodes `setting_key =
  'all'`; office/facility setting is only available by joining
  `pricing_provider_procedure_location` where `place_of_service` survived the
  import. If the location table is absent or incomplete, publish only
  `setting_key='all'`.
- `taxonomy_code varchar not null`
- `taxonomy_grouping varchar null`
- `taxonomy_classification varchar null`
- `taxonomy_specialization varchar null`
- `npi_count bigint not null`
- `primary_taxonomy_npi_count bigint not null`
- `total_services double precision not null`
- `total_beneficiaries double precision null`
- `share_of_code_npis double precision not null`
- `share_of_code_services double precision not null`
- `provider_type_distribution jsonb not null default '{}'::jsonb`
- `evidence_flags jsonb not null default '{}'::jsonb`
- `source varchar not null` (`cms_provider_service_claims`)
- `source_year int not null`
- `updated_at timestamptz not null`

Primary key:

```sql
(code_system, code, year, setting_key, taxonomy_code)
```

Important indexes:

```sql
(code_system, code, year, setting_key, share_of_code_npis desc)
(taxonomy_code, year)
(procedure_code, year, setting_key)
```

### `mrf.procedure_taxonomy_profile`

Resolved code-level profile before free-text clinical intent is applied. This
table is derived from signal rows plus reviewed overrides. It should only store
the code's default evidence profile; intent-specific logic belongs in the
override table and is applied at resolver query time.

Recommended columns:

- `code_system varchar not null`
- `code varchar not null`
- `year int not null`
- `setting_key varchar not null`
- `candidate_taxonomy_codes varchar[] not null`
- `recommended_mode varchar not null` (`hard_filter`, `soft_boost`,
  `validate_only`, `ambiguous`)
- `confidence varchar not null` (`high`, `medium`, `low`, `ambiguous`)
- `representativeness varchar not null` (`ok`, `thin`, `biased`, `unknown`)
- `needs_intent boolean not null default false`
- `resolution_reason varchar not null`
- `evidence jsonb not null`
- `needs_review boolean not null default false`
- `source varchar not null`
- `updated_at timestamptz not null`

Primary key:

```sql
(code_system, code, year, setting_key)
```

### `mrf.procedure_taxonomy_override`

Small curated layer for clinical policy decisions that cannot be safely learned
from utilization alone.

Recommended columns:

- `code_system varchar not null`
- `code varchar not null`
- `setting_key varchar not null default 'all'`
- `intent_key varchar not null default '__any__'`
- `override_type varchar not null` (`allow`, `deny`, `replace`,
  `mark_ambiguous`, `soft_boost`, `hard_filter`)
- `taxonomy_codes varchar[] null`
- `taxonomy_classification varchar null`
- `include_subspecialties boolean not null default false`
- `representativeness varchar null`
- `confidence varchar null`
- `review_status varchar not null default 'proposed'`
- `reason text not null`
- `source_url text null`
- `reviewed_by varchar null`
- `reviewed_at timestamptz null`
- `active boolean not null default true`
- `updated_at timestamptz not null`

This table replaces growing Python constants for clinical exceptions. The code
may still carry a tiny emergency fallback for bootstrap/test mode, but normal
runtime should read the database.

`intent_key` is a controlled key such as `primary_care_acute`, `flu`,
`orthopedic_sports`, or `dental_general`. It is not arbitrary user free text.
The resolver maps `clinical_intent` text to one or more intent keys at query
time, then applies reviewed overrides on top of the code-level profile.

## Derivation Algorithm

For each active claims-pricing year:

1. Resolve external code:
   - `pricing_provider_procedure.reported_code` is the source-observed HCPCS/CPT
     code.
   - Normalize numeric five-character codes as `CPT` for UI/API convenience,
     but keep `HCPCS` equivalence where the existing code system layer expects
     it.
   - Keep CDT separate. Medicare Part B has weak dental coverage, so CDT
     mappings should usually be curated or learned from dental-specific sources,
     not overfit from Medicare claims.
2. Join observed NPIs:
   - `pricing_provider_procedure pp`
   - `pricing_provider p on p.npi = pp.npi and p.year = pp.year`
   - prefer `pricing_provider_quality_feature qf on qf.npi = pp.npi and
     qf.year = pp.year` for the selected primary taxonomy code and taxonomy
     classification when the provider-quality materialization is available;
   - fall back to direct `npi_taxonomy nt on nt.npi = pp.npi`, preferring
     `nt.healthcare_provider_primary_taxonomy_switch = 'Y'`;
   - keep a secondary-taxonomy path for soft matching, mismatch analysis, and
     false-negative protection;
   - join `nucc_taxonomy nucc on nucc.code = taxonomy_code`.
3. Aggregate by:
   - year,
   - code system/code,
   - place of service/setting,
   - taxonomy code.
   Store CMS provider-type distributions as JSON evidence; do not put provider
   type in the aggregate primary key.
4. Compute shares:
   - distinct-NPI share within the code/setting/year,
   - service share within the code/setting/year,
   - minimum provider count,
   - total beneficiary count,
   - suppressed/missing-cell indicators.
5. Select candidate taxonomies:
   - require a configurable minimum NPI count, e.g. `20` for full imports;
   - rank primarily by distinct-NPI share, not service share;
   - use service share only as secondary evidence because it rewards high-volume
     adult specialists;
   - include enough ranked taxonomies to cover a target cumulative NPI share,
     e.g. `0.85`;
   - mark `ambiguous` when the code is broad and no taxonomy dominates;
   - mark `low` confidence when only secondary taxonomy evidence exists;
   - mark `thin` or `biased` representativeness when total beneficiaries,
     distinct NPIs, source suppression, code family, or reviewed metadata shows
     Medicare FFS is insufficient.
6. Apply overrides:
   - `replace` for known clinical-intent cases;
   - `deny` for supplier/facility codes where a physician taxonomy is wrong;
   - `mark_ambiguous` for broad office visits without a care intent;
   - `hard_filter` only for reviewed cases where false-negative risk is
     acceptable.
7. Publish `procedure_taxonomy_profile` rows with evidence snapshots so API
   responses are explainable.

## Representativeness Gate

Every resolved code must pass a representativeness gate before the resolver can
return `recommended_mode = hard_filter`.

Minimum gate inputs:

- `total_beneficiaries` and `npi_count` for the code/setting/year;
- percent of rows with suppressed or missing counts;
- distinct-NPI share of candidate taxonomy codes;
- service-share/NPI-share divergence;
- whether the code is known or suspected to be young-skew, pediatric, OB,
  sports-medicine, dental, or otherwise poorly represented by Medicare FFS;
- whether a reviewed override exists.

Default policy:

- `representativeness=ok` and `confidence=high` may return `hard_filter` only
  for reviewed low-false-negative-risk cases.
- `confidence=medium` should return `soft_boost`, not a hard filter.
- `representativeness=thin`, `biased`, or `unknown` must return `soft_boost`,
  `validate_only`, or `ambiguous` unless a reviewed override explicitly says
  otherwise.
- Fallback must fire on thin or biased *presence*, not only complete absence of
  CMS evidence. A sparse adult-only signal for a young-skew code is not enough
  to exclude other taxonomy candidates.

The ACL example is the canonical test case: `29888` may have weak Medicare FFS
evidence but a clinically valid orthopedic/sports-medicine taxonomy override.
The resolver should surface that override as reviewed curated evidence instead
of pretending Medicare alone proved the mapping.

## Filter Modes

The resolver returns a `recommended_mode`; callers must honor it.

- `hard_filter`: safe to pass `taxonomy_codes` into
  `/pricing/providers/search-by-procedure` or `/pricing/group-plan-providers` as
  a hard NPPES taxonomy predicate. This mode is allowed only after the
  representativeness gate passes or a reviewed override opts in.
- `soft_boost`: do not use taxonomy as a hard pre-filter. Fetch broader
  plan-priced candidates, then rank providers with matching primary taxonomy,
  secondary taxonomy, CMS provider type, or reviewed override evidence above
  weaker matches.
- `validate_only`: do not narrow results. Use taxonomy evidence only to label
  and warn when a candidate looks off-specialty.
- `ambiguous`: ask for clinical intent or explicit specialty/taxonomy before
  recommending providers.

Post-query validation is mandatory for member-facing results in every mode,
including `hard_filter`. It catches false positives and produces explainable
evidence. For false-negative protection, `soft_boost` and `validate_only` must
not discard providers solely because the primary taxonomy is missing or generic.

## Ambiguity Rules

### Broad E/M codes

Codes such as `99213`, `99214`, `99203`, and `99204` should not auto-resolve to
primary care when the only input is the code. They should return:

```json
{
  "confidence": "ambiguous",
  "needs_intent": true,
  "message": "Office visit codes are billed by many specialties; pass clinical_intent or explicit taxonomy_codes."
}
```

When the input includes a care intent such as `flu`, `strep`, `UTI`, or
`primary care visit`, the resolver may choose the curated primary-care taxonomy
group and still include the utilization evidence for transparency.

### Specialist procedure ranges

Existing code-range heuristics in `api/ptg2_code_filters.py` should become
seed overrides or test fixtures, then be validated by the derived signal table.
If a range-derived rule conflicts with CMS/NPPES utilization, the resolver
should surface the conflict as `needs_review=true` rather than silently choosing
one side.

Near term, these hardcoded rules may remain load-bearing while the resolver is
implemented. They should still be reviewed for false-negative risk: if they
auto-apply as hard filters, they can exclude a legitimate provider with missing
or generic primary taxonomy. Prefer reviewed overrides with `soft_boost` unless
the rule has been accepted for hard filtering.

### Advanced practice providers

Nurse practitioner and physician assistant taxonomy is provider-type-like and
often specialty-agnostic. Treat broad `363L00000X` / `363A00000X` carefully:

- include specialized NP taxonomy codes when they are specific enough;
- include PA/NP in primary-care intent only when the evidence or override says
  this code/setting is primary-care appropriate;
- never let generic PA/NP taxonomy be the only reason to recommend the provider
  for a specialist procedure.

## API Surface

Add these endpoints to `healthcare-mrf-api`; external gateways may proxy them
without changing the contract.

### `GET /api/v1/pricing/procedure-taxonomy/resolve`

Parameters:

- `code` required
- `code_system` optional (`CPT`, `HCPCS`, `CDT`)
- `year` optional
- `setting` / `place_of_service` optional
- `clinical_intent` optional
- `include_evidence` optional bool

Response:

```json
{
  "ok": true,
  "code_system": "CPT",
  "code": "99214",
  "year": 2024,
  "setting_key": "office",
  "intent_key": "flu",
  "confidence": "high",
  "representativeness": "ok",
  "recommended_mode": "hard_filter",
  "provider_filter": {
    "taxonomy_codes": ["207Q00000X", "207R00000X", "208D00000X", "208000000X"],
    "include_subspecialties": false,
    "primary_only": true
  },
  "provider_boost": null,
  "needs_review": false,
  "evidence": {
    "source": "cms_provider_service_claims+nppes+nucc",
    "basis": ["reviewed_intent_override", "cms_provider_service_claims", "nppes_primary_taxonomy"],
    "top_taxonomies": []
  }
}
```

For a thin/biased but useful mapping:

```json
{
  "ok": true,
  "code_system": "CPT",
  "code": "29888",
  "year": 2024,
  "setting_key": "all",
  "intent_key": "orthopedic_sports",
  "confidence": "medium",
  "representativeness": "thin",
  "recommended_mode": "soft_boost",
  "provider_filter": null,
  "provider_boost": {
    "taxonomy_codes": ["207X00000X", "207XX0005X"],
    "secondary_taxonomy_allowed": true,
    "cms_provider_types": ["Orthopedic Surgery", "Sports Medicine"]
  },
  "needs_review": false,
  "evidence": {
    "source": "reviewed_override+cms_provider_service_claims+nppes+nucc",
    "basis": ["reviewed_override", "thin_medicare_evidence"]
  }
}
```

If ambiguous:

```json
{
  "ok": true,
  "code_system": "CPT",
  "code": "99214",
  "confidence": "ambiguous",
  "representativeness": "biased",
  "recommended_mode": "ambiguous",
  "needs_intent": true,
  "provider_filter": null,
  "provider_boost": null,
  "message": "Pass clinical_intent or explicit taxonomy_codes for broad office visit codes."
}
```

### `GET /api/v1/pricing/procedure-taxonomy/evidence`

Parameters:

- same code/year/setting inputs,
- pagination for top taxonomy/provider-type rows.

Returns ranked signal rows, not just the chosen filter. This endpoint is for
admin review, debugging, and explaining why a filter was chosen.

### `POST /api/v1/pricing/procedure-taxonomy/resolve-batch`

Accepts a list of codes and returns the same resolution payload per code.
Automation clients should use this when a care-intent resolver returns office
visit plus lab/test codes.

## API Consumer Changes

API consumers should stop carrying the normal runtime mapping.

1. Add a tool:

   ```text
   resolve_procedure_taxonomy(code, code_system, clinical_intent?, setting?)
   ```

2. Update `resolve_care_codes`:
   - still return the billing codes for a care intent;
   - call or instruct callers to call procedure-taxonomy resolution for each
     code;
   - return `recommended_provider_filter` only when all codes share a compatible
     `hard_filter`;
   - return `recommended_provider_boost` when the resolver says `soft_boost`;
   - return a blocking ambiguity message when any required code returns
     `ambiguous`.

3. Update `search_providers_by_procedure` guidance:
   - for plan-scoped member searches, pass resolved `taxonomy_codes` into the
     pricing call only when `recommended_mode=hard_filter`;
   - when `recommended_mode=soft_boost`, do not pre-filter; fetch plan-priced
     candidates and rank matching primary taxonomy, secondary taxonomy, CMS
     provider type, and reviewed override evidence higher;
   - remove the current blanket instruction that says never to pass
     classification. Replace it with mode-aware guidance;
   - keep post-query validation mandatory, not optional.

4. Keep existing hardcoded lists only for:
   - bootstrap without resolver endpoint,
   - unit tests,
   - explicit fallback when the resolver returns unavailable.

## External Gateway Changes

External gateways should forward explicit taxonomy parameters for:

- `/api/v1/pricing/group-plan-providers`
- `/api/v1/pricing/providers/search-by-procedure`
- `/api/v1/pricing/provider-specialties`

Add proxy routes for:

- `GET /api/v1/pricing/procedure-taxonomy/resolve`
- `GET /api/v1/pricing/procedure-taxonomy/evidence`
- `POST /api/v1/pricing/procedure-taxonomy/resolve-batch`

Implementation requirements:

- register each route explicitly in the gateway configuration;
- require the appropriate authorization scope for
  `/api/v1/pricing/procedure-taxonomy`;
- keep the same path and authorization mapping in automation clients;
- add entitlement tests beside the existing provider-specialty/group-plan tests;
- expose these in OpenAPI with examples that show ambiguous office visit
  behavior and `soft_boost` behavior.

## Runtime Search Flow

For member-facing plan search:

1. Resolve group plan/subscription.
2. Resolve care intent into candidate codes (`resolve_care_codes`).
3. Resolve each code to taxonomy evidence:
   - if the code is ambiguous and no clinical intent exists, ask for intent or
     require explicit specialty;
   - if resolver modes conflict across codes, use the safest common mode:
     `ambiguous` > `validate_only` > `soft_boost` > `hard_filter`;
   - if a required code has only thin/biased evidence, do not hard-filter unless
     a reviewed override explicitly permits it.
4. Call plan-scoped provider pricing:
   - `plan_id`,
   - `market_type=group`,
   - `code`,
   - location/radius,
   - `include_providers=true`.
   Add `taxonomy_codes` and `include_subspecialties=false` only for
   `recommended_mode=hard_filter`.
5. Validate returned providers against primary taxonomy, secondary taxonomy,
   CMS provider type, and override evidence. Do not drop a provider solely
   because primary taxonomy is generic/missing unless the mode was
   `hard_filter`.
6. Rank by distance, negotiated rate, taxonomy/override match quality, and
   available quality/cost signals.

## Rollout Plan

1. Add tables/views and SQL materialization behind `procedure-taxonomy`.
   - Use Alembic for schema changes and update SQLAlchemy model declarations in
     `db/models/_legacy.py` or the current model home used by the importer.
   - Use versioned live/stage names or a feature-flagged cutover so a bad build
     cannot replace the current resolver profile silently.
   - Use idempotent `ON CONFLICT`/swap semantics, advisory locks for publish,
     row-count floors, row-count-delta abort guards, and diagnostics for
     taxonomy coverage, null taxonomy rate, and representativeness buckets.
   - Keep the build cancellable through the operator conventions used by
     other long-running importers.
2. Run against the current local/test claims import to validate table shape.
3. Run against a full dev claims-pricing year and inspect:
   - `99213`/`99214` ambiguity,
   - `87804` primary-care/lab split,
   - `29888` ACL/orthopedic curated override behavior,
   - imaging/radiology codes,
   - lab/pathology codes,
   - PT/OT codes,
   - dental/CDT unavailable/curated behavior.
4. Add resolver endpoints in `healthcare-mrf-api`.
5. Add external-gateway proxy routes, authorization mapping, OpenAPI, and entitlement
   tests.
6. Update automation clients to call the resolver and follow the published
   authorization mapping.
7. Replace or demote `api/ptg2_code_filters.py` inferred rules to overrides.

## Acceptance Criteria

1. For a broad office-visit code without clinical intent, the resolver returns
   `confidence=ambiguous`, `recommended_mode=ambiguous`, and no
   `provider_filter`.
2. For `clinical_intent=flu` plus office visit/test codes, the resolver returns
   a primary-care-compatible result with evidence and a safe
   `recommended_mode`. It may be `hard_filter` only if the representativeness
   gate or reviewed override permits it.
3. For young-skew/thin-Medicare evidence, e.g. ACL `29888`, the resolver does
   not emit a Medicare-only high-confidence hard filter. It either uses a
   reviewed orthopedic override as `soft_boost`/reviewed `hard_filter`, or
   returns `needs_review`.
4. A synthetic provider with a legitimate specialty signal but missing/generic
   primary NPPES taxonomy is not silently excluded in `soft_boost` or
   `validate_only` mode.
5. For a specialist-owned code with representative evidence, the resolver emits
   taxonomy evidence derived from CMS/NPPES or a curated override, not a Python
   hardcoded range alone.
6. `search_providers_by_procedure` uses taxonomy as a hard plan-scoped filter
   only when `recommended_mode=hard_filter`; otherwise it broad-searches,
   ranks/boosts, and validates.
7. API consumers no longer need a private hardcoded list to decide the correct
   doctor taxonomy for a code in normal runtime.
8. Every resolved response includes enough evidence to audit why the taxonomy
   mode and taxonomy candidates were chosen.
9. The new public paths are gated by the documented authorization scope
   and covered by entitlement tests.
10. Materialization fails closed on row-count floor/delta, taxonomy coverage, or
    representativeness diagnostics that fall outside configured bounds.

## Open Questions

- Dental/CDT needs a separate evidence strategy. Medicare Part B is not enough
  to infer most dental provider mappings.
- Some provider types are facilities or suppliers, not individual doctors. The
  resolver should distinguish `provider taxonomy for who can perform this` from
  `facility/supplier taxonomy for where it is billed`.
- If we license AMA/ADA content later, the licensed reference layer should enrich
  labels and hierarchy but should still not replace observed utilization
  evidence.
