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

The Aetna API is not currently crawlable through the generic unfiltered FHIR
search strategy:

- `Practitioner` requires NPI or name plus location.
- `PractitionerRole` and `OrganizationAffiliation` require specialty.
- `Location` and `InsurancePlan` can be queried by location/state.
- The observed CapabilityStatement does not advertise a root `$export`
  operation, and the generic `[base]/$export?_type=...` path returned 404 from
  dev.

Therefore Aetna credentials can reduce the credential backlog immediately, but
full Aetna row coverage needs a dedicated Aetna partitioning strategy or the
exact API-gateway bulk export route from Aetna's product swagger.
