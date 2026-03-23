# Data Source Registry (Authoritative)

This spec defines the authoritative source-of-truth policy for `healthcare-mrf-api` data provenance.

## Scope

This registry covers active import pipelines in this repository and maps:

1. external source website family,
2. dataset/feed class,
3. owning importer command,
4. primary output tables or API-facing dataset layers.

It does not replace per-import operational runbooks under `docs/imports/`.

## Authoritative Registry Location

The canonical public registry is maintained at:

- `docs/data-sources.md`

Changes to import source behavior must update `docs/data-sources.md` in the same pull request.

## Source Governance Rules

1. External sources are import-time dependencies; request-time API handlers must remain local-DB first.
2. Every new importer must have:
   - one entry in `docs/imports/README.md`,
   - one per-import runbook under `docs/imports/`,
   - one source mapping entry in `docs/data-sources.md`.
3. If an importer changes source families (for example, adds a new catalog endpoint), documentation updates are required in the same change.
4. Derived/materialized imports that use only local tables (no external fetch) must be explicitly marked as internal-source imports.
5. Environment-variable URL overrides (`HLTHPRT_*_URL`) are implementation details; docs must still describe the default public source website.

## Current Active Source Families (Summary)

- CMS Data Portal (`data.cms.gov`)
- CMS Provider Data Catalog (`data.cms.gov/provider-data`)
- CMS Marketplace Public Use Files (federal + state-based)
- NPPES
- CMS Medicare FFS Provider Enrollment
- CMS QPP
- CDC SVI
- CDC PLACES
- U.S. Census APIs
- Census LEHD/LODES
- HUD USPS crosswalk API + Census tract/ZCTA fallback file
- HRSA downloads
- Medicaid Data Portal (SDUD/NADAC/FUL)
- NUCC taxonomy
- FDA BeSafeRx + state board machine-readable endpoints
- Repository-bundled geo seed support file
- Configured payer TiC TOC/rate/provider-reference URLs

## Cross-Repo Note

For drug-only sources (OpenFDA, RxNorm, DailyMed context), see:

- `../drug-api/docs/data-sources.md` (in the `drug-api` repository)

