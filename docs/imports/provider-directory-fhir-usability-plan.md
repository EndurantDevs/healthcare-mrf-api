# Provider Directory FHIR Usability Plan

This note captures the current split between work needed for broad future
coverage and the shorter path needed to make the existing Provider Directory
data usable in search and PTG evidence now.

## Defer For Future Full Coverage

- Credential onboarding remains the main coverage gap. The live dev audit on
  2026-07-02 showed 796 Provider Directory sources, with 668 still requiring
  auth or registration and 32 responding with non-FHIR content.
- Non-Aetna credentialed payers should be onboarded in priority order from the
  coverage audit credential backlog exports. Each rule must be host/API-base
  scoped, secret-backed, and verified by a probe plus at least one resource
  fetch.
- Non-FHIR seed URLs should either be resolved to a concrete FHIR base through
  supplemental catalog parsing or marked as explicit blocked/non-importable
  sources so the audit distinguishes known blockers from unresolved discovery.
- Payers with non-crawlable search contracts need source-specific partitioners.
  A generic unfiltered `Resource?_count=N` scan is not enough for every payer.

## Usable-Now Requirements

- Provider Directory address overlays must publish normalized country codes.
  `001`, `840`, `USA`, and `United States` should surface as `US`.
- The compact Provider Directory address overlay is the serving/search path for
  address-key and phone lookups. API paths should use this indexed overlay or
  derived serving tables, not broad `entity_address_unified.address_sources`
  scans.
- Provider Directory network catalog and PTG corroboration must be materialized
  after Provider Directory imports so pricing responses can expose payer
  directory evidence without live FHIR joins.
- The default recurring chain is monthly: full Provider Directory import,
  Provider Directory partial unified-address projection, then Provider
  Directory corroboration publish.

## Aetna Exception

Aetna credentials are available on the dev server and can be mounted through the
normal `provider-directory-credentials` secret. Aetna's production Provider
Directory token endpoint accepts OAuth2 client credentials with scope
`Public NonPII`.

Aetna exposes two production Provider Directory bases:

- `/fhir/v1/providerdirectory` for Medicaid Provider Directory resources.
- `/fhir/v1/providerdirectorydata` for Commercial and Medicare Provider
  Directory resources.

The Commercial/Medicare base supports Bulk Data `$export` with
`Prefer: respond-async`, but the Aetna gateway rejects the optional
`_outputFormat=application/fhir+ndjson` parameter. The importer therefore omits
`_outputFormat` for that source while preserving the generic Bulk Data request
shape for other payers. This is the full-refresh path for Aetna
Commercial/Medicare data.

Aetna is still not crawlable through a generic unfiltered FHIR search strategy:

- `Practitioner` requires NPI or name plus location.
- `PractitionerRole` and `OrganizationAffiliation` require specialty.
- `Location` and `InsurancePlan` can be queried by location/state.
- The Medicaid base is constrained to specific search criteria and does not
  expose the Commercial/Medicare bulk route.

For bounded non-bulk validation, the importer uses Aetna-specific state
partitions for Commercial/Medicare `Practitioner`, `Organization`, and
`Location`, plus `name=aetna` search for `InsurancePlan`. Enumerating all
two-letter name combinations or all
NPIs is not a full import strategy; NPI search is only useful when we already
have known NPIs. ZIP search can support targeted checks, but a full ZIP crawl is
less efficient than Aetna's bulk route and still does not solve every role or
affiliation expansion cleanly.
