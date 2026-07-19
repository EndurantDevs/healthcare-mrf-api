# healthcare-mrf-api

`healthcare-mrf-api` is an open data-ingestion and API platform for public U.S. healthcare pricing, provider, plan, pharmacy, and Medicare-derived benchmarking data.

It combines multiple public programs into one operational service so applications can query provider directories, plan data, procedure pricing, pharmacy activity, quality benchmarks, and enrollment-derived enrichment from a consistent API layer.

## What This Repository Provides

The service brings together:

- marketplace plan and issuer data
- provider directory and taxonomy data
- Medicare physician and Part D claims aggregates
- provider quality and benchmark scoring inputs
- Medicare provider enrollment enrichment
- Part D formulary and pharmacy network activity
- pharmacy license normalization across state board sources
- official-code clinical condition, treatment, and clinical-area lookup support
- ZIP, city, state, and radius-based geographic lookup support

Typical use cases include:

- provider search and directory enrichment
- procedure and prescription cost benchmarking
- pharmacy activity and network lookup
- plan/provider matching and pricing workflows
- provider quality analytics and curation
- healthcare data delivery for APIs, internal systems, and AI-agent integrations

## Source Families

This repository resolves and processes data from public source websites, including:

- [CMS Data Portal](https://data.cms.gov/)
- [CMS Provider Data Catalog](https://data.cms.gov/provider-data/)
- [CMS Marketplace Public Use Files](https://www.cms.gov/marketplace/resources/data/public-use-files)
- [CMS State-based Marketplace Public Use Files](https://www.cms.gov/marketplace/resources/data/state-based-public-use-files)
- [NPPES NPI Files](https://download.cms.gov/nppes/NPI_Files.html)
- [CMS Medicare Fee-for-Service Public Provider Enrollment](https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/medicare-fee-for-service-public-provider-enrollment)
- [CMS Quality Payment Program](https://www.cms.gov/Medicare/Quality-Payment-Program/Quality-Payment-Program.html)
- [U.S. Census Data API](https://api.census.gov/data.html)
- [Census LEHD / LODES](https://lehd.ces.census.gov/data/)
- [HUD USPS crosswalk API](https://www.huduser.gov/hudapi/public/usps?type=2&query=All)
- [CDC / ATSDR Social Vulnerability Index](https://www.atsdr.cdc.gov/place-health/php/svi/index.html)
- [CDC PLACES ZCTA Data](https://chronicdata.cdc.gov/500-Cities-Places/PLACES-Local-Data-for-Better-Health-ZCTA-Data-2025/qnzd-25i4/about_data)
- [HRSA data download](https://data.hrsa.gov/data/download)
- [Medicaid Data Portal](https://data.medicaid.gov/)
- [NUCC Provider Taxonomy](https://www.nucc.org/index.php/21-provider-taxonomy)
- [FDA BeSafeRx state board directory](https://www.fda.gov/drugs/besaferx-your-source-online-pharmacy-information/locate-state-licensed-online-pharmacy)
- state pharmacy board websites discovered from FDA BeSafeRx
- MRF source registries and TPA-hosted TiC metadata indexes, documented in [docs/data-sources.md](./docs/data-sources.md)

See the full source catalog in [docs/data-sources.md](./docs/data-sources.md).

Readability and ownership rules live in `docs/architecture.md` and
`docs/readability.md`. Run `python scripts/readability_budget.py` before commits
to confirm a change does not add new readability debt. Runtime Python-changing
pull requests must reduce total readability debt by at least 1% until it reaches
zero. Maintainers may apply `readability-zero-growth-approved` to a focused
change when unrelated refactoring would raise its risk; net and protected debt
growth remain blocked. CI and test tooling must not add debt. Commit message
rules live in `docs/commit-messages.md`; run
`python3 scripts/check_commit_messages.py --last 1` before pushing hand-written
commits.

Current Python and PTG2 scanner coverage, their measured scopes, and the soft
coverage-growth CI rule are documented in `docs/test-coverage.md`.

## Importers

Each importer is a separate operational pipeline. Together, they build the canonical data used by the API.

| Importer | Start command | Primary source family | Main output |
| --- | --- | --- | --- |
| `mrf` | `python main.py start mrf` | CMS marketplace public use files | issuer, plan, formulary, network, and MRF-linked marketplace data |
| `plan-attributes` | `python main.py start plan-attributes` | CMS marketplace plan attributes files | plan metadata, prices, benefits, and rating-area data |
| `ptg` | `python main.py start ptg` | Transparency in Coverage machine-readable files | TiC table-of-contents and rate-file ingestion |
| `mrf-source-discovery` | `python main.py start mrf-source-discovery` | payer, network, and TPA-hosted TiC source registries | searchable MRF source, plan, and file metadata catalog for PTG targeting |
| `code-sets` | `python main.py start code-sets` | CMS POS + CMS Blue Button RC code pages | readable Revenue Center and Place of Service labels in `code_catalog` |
| `ms-drg` | `python main.py start ms-drg` | CMS MS-DRG definitions manual | MS-DRG labels and MS-DRG to ICD-10-CM/PCS grouping relationships |
| `clinical-reference` | `python main.py start clinical-reference` | official condition/procedure code files | condition, treatment, clinical-area, and crosswalk tables for Ribbon-replacement APIs |
| `npi` | `python main.py start npi` | NPPES | provider identity, addresses, taxonomies, and directory search data |
| `nucc` | `python main.py start nucc` | NUCC | provider taxonomy lookup tables |
| `geo` | `python main.py start geo` | repository support files | ZIP/city/state/coordinate lookup tables |
| `geo-census` | `python main.py start geo-census` | U.S. Census APIs | ZIP/ZCTA demographic and economic profile metrics |
| `openaddresses` | `python main.py start openaddresses` | OpenAddresses Batch | US-only local address geocode cache and missing archive coordinate backfill |
| `claims-pricing` | `python main.py start claims-pricing` | CMS Medicare physician claims | provider procedure pricing, peer benchmarks, and procedure geography |
| `claims-procedures` | `python main.py start claims-procedures` | CMS Medicare physician claims | alias of `claims-pricing` |
| `drug-claims` | `python main.py start drug-claims` | CMS Medicare Part D claims | provider-drug and prescription claims aggregates |
| `provider-quality` | `python main.py start provider-quality` | CMS QPP, CDC SVI, Medicare claims | provider quality measures, domain scores, peer targets, and rankings |
| `places-zcta` | `python main.py start places-zcta` | CDC PLACES | ZIP/ZCTA health indicator measures |
| `lodes` | `python main.py start lodes` | Census LEHD/LODES | workplace demand aggregates by ZCTA with tract-to-ZIP crosswalk validation |
| `medicare-enrollment` | `python main.py start medicare-enrollment` | CMS Medicare Enrollment Dashboard | county-canonical Medicare/Part D enrollment plus ZIP-allocated coverage table |
| `cms-doctors` | `python main.py start cms-doctors` | CMS Doctors & Clinicians | provider-to-practice address coverage for supply scoring |
| `facility-anchors` | `python main.py start facility-anchors` | HRSA + CMS hospitals | FQHC and hospital facility anchor dataset |
| `pharmacy-economics` | `python main.py start pharmacy-economics` | Medicaid SDUD + NADAC + FUL | state/NDC pharmacy margin reference dataset |
| `entity-address-unified` | `python main.py start entity-address-unified` | internal imported tables (`npi`, `provider-enrichment`, `cms-doctors`, `facility-anchors`) | unified address/entity search materialization with deterministic inferred-NPI mapping |
| `provider-enrichment` | `python main.py start provider-enrichment` | CMS Medicare FFS public provider enrollment | PECOS-based provider enrollment and relationship enrichment |
| `partd-formulary-network` | `python main.py start partd-formulary-network` | CMS Part D public files | formulary coverage, pharmacy activity, and pharmacy network data |
| `pharmacy-license` | `python main.py start pharmacy-license` | FDA/state board sources | normalized pharmacy license and board-status data |

Detailed run instructions for every importer are documented in [docs/imports/README.md](./docs/imports/README.md).

## Import Commands (Complete)

Run from repo root in an activated virtualenv.

### Queue-based imports

| Import | Enqueue | Worker (drain queue) | Finalize / publish |
| --- | --- | --- | --- |
| `mrf` | `python main.py start mrf` | `python main.py worker process.MRF --burst` | `python main.py worker process.MRF_finish --burst` or `python main.py finish mrf` |
| `plan-attributes` | `python main.py start plan-attributes` | `python main.py worker process.Attributes --burst` | publish handled in worker shutdown |
| `npi` | `python main.py start npi` | `python main.py worker process.NPI --burst` | `python main.py worker process.NPI_finish --burst` |
| `nucc` | `python main.py start nucc` | `python main.py worker process.NUCC --burst` | publish handled in worker shutdown |
| `claims-pricing` | `python main.py start claims-pricing [--import-id YYYYMMDD] [--test]` | `python main.py worker process.ClaimsPricing --burst` | `python main.py worker process.ClaimsPricing_finish --burst` or `python main.py finish claims-pricing --import-id <import_id> --run-id <run_id> [--test]` |
| `claims-procedures` | `python main.py start claims-procedures [--import-id YYYYMMDD] [--test]` | `python main.py worker process.ClaimsPricing --burst` | `python main.py worker process.ClaimsPricing_finish --burst` or `python main.py finish claims-procedures --import-id <import_id> --run-id <run_id> [--test]` |
| `drug-claims` | `python main.py start drug-claims [--import-id YYYYMMDD] [--test]` | `python main.py worker process.DrugClaims --burst` | `python main.py worker process.DrugClaims_finish --burst` or `python main.py finish drug-claims --import-id <import_id> --run-id <run_id> [--test]` |
| `provider-quality` | `python main.py start provider-quality [--import-id YYYYMMDD] [--test]` | `python main.py worker process.ProviderQuality --burst` | `python main.py worker process.ProviderQuality_finish --burst` or `python main.py finish provider-quality --import-id <import_id> --run-id <run_id> [--test]` |
| `provider-enrichment` | `python main.py start provider-enrichment [--test]` | `python main.py worker process.ProviderEnrichment --burst` | `python main.py worker process.ProviderEnrichment_finish --burst` |
| `partd-formulary-network` | `python main.py start partd-formulary-network [--import-id YYYYMMDD] [--test]` | `python main.py worker process.PartDFormularyNetwork --burst` | `python main.py worker process.PartDFormularyNetwork_finish --burst` or `python main.py finish partd-formulary-network --import-id <import_id> --run-id <run_id> [--test]` |
| `pharmacy-license` | `python main.py start pharmacy-license [--import-id YYYYMMDD] [--test]` | `python main.py worker process.PharmacyLicense --burst` | `python main.py worker process.PharmacyLicense_finish --burst` or `python main.py finish pharmacy-license --import-id <import_id> --run-id <run_id> [--test]` |
| `places-zcta` | `python main.py start places-zcta [--test]` | `python main.py worker process.PlacesZcta --burst` | `python main.py worker process.PlacesZcta_finish --burst` |
| `lodes` | `python main.py start lodes [--test]` | `python main.py worker process.LODES --burst` | publish handled in worker shutdown |
| `medicare-enrollment` | `python main.py start medicare-enrollment [--test]` | `python main.py worker process.MedicareEnrollment --burst` | publish handled in worker shutdown |
| `cms-doctors` | `python main.py start cms-doctors [--test]` | `python main.py worker process.CMSDoctors --burst` | publish handled in worker shutdown |
| `facility-anchors` | `python main.py start facility-anchors [--test]` | `python main.py worker process.FacilityAnchors --burst` | publish handled in worker shutdown |
| `pharmacy-economics` | `python main.py start pharmacy-economics [--test]` | `python main.py worker process.PharmacyEconomics --burst` | publish handled in worker shutdown |
| `entity-address-unified` | `python main.py start entity-address-unified [--test]` | `python main.py worker process.EntityAddressUnified --burst` | publish handled in worker shutdown |
| `openaddresses` | `python main.py start openaddresses [--test] [--backfill-only]` | `python main.py worker process.OpenAddresses --burst` | publish/backfill handled in worker shutdown |

### Direct (non-ARQ) imports

| Import | Command |
| --- | --- |
| `ptg` | `python main.py start ptg [--test]` |
| `code-sets` | `python main.py start code-sets [--test]` |
| `ms-drg` | `python main.py start ms-drg [--test] [--relationship-page-limit N]` |
| `clinical-reference` | `python main.py start clinical-reference [--test] [--import-id YYYYMMDD]` |
| `geo` | `python main.py start geo [--file /path/to/geo_city_public.csv]` |
| `geo-census` | `python main.py start geo-census [--test]` |
| `openaddresses` | `python main.py start openaddresses [--test] [--backfill-only]` |

Operational note:
- Do **not** run `ClaimsPricing_finish` and `DrugClaims_finish` at the same time. Both touch shared `code_catalog` and `code_crosswalk`.
- `openaddresses` imports only U.S. sources from OpenAddresses Batch, then backfills `address_archive_v2` in this order: exact house/street/state/ZIP, strict fuzzy street match, then relaxed city/state/ZIP-scoped street match.
- `openaddresses` full imports process multiple OpenAddresses source files concurrently; tune with `HLTHPRT_OPENADDRESSES_SOURCE_CONCURRENCY`.
- `openaddresses` stages valid U.S. point-address rows that are missing ZIP separately, restores ZIP5 from `tiger.zcta5` only when the point maps to exactly one state-matching ZIP, then publishes those recovered rows with `zip5_source='tiger_zcta_point'`. Tune restore parallelism with `HLTHPRT_OPENADDRESSES_ZIP_RESTORE_CONCURRENCY` and bucket count with `HLTHPRT_OPENADDRESSES_ZIP_RESTORE_SHARDS`.
- For faster dev catch-up, multiple `openaddresses` load shards can use the same `--import-id`/`import_id` (or `HLTHPRT_IMPORT_ID_OVERRIDE`) with `--resume-stage`, `--load-only`, and non-overlapping `--start-index`/`--end-index`; run one `--publish-only --import-id <same-id>` job after all shards complete.
- `openaddresses` archive backfill automatically shards missing `address_archive_v2` rows by state and ZIP prefix after publish. Tune database backfill parallelism with `HLTHPRT_OPENADDRESSES_BACKFILL_CONCURRENCY` and shard size with `HLTHPRT_OPENADDRESSES_BACKFILL_ZIP_PREFIX_LENGTH`; explicit `HLTHPRT_OPENADDRESSES_BACKFILL_STATE_CODE` / `HLTHPRT_OPENADDRESSES_BACKFILL_ZIP_PREFIX` still run a targeted shard. Each shard only updates rows that still have no coordinates, so retries are safe.
- Address canonicalization for source imports (MRF/PTG/NPI and similar staging tables) can restore blank ZIPs from existing coordinates before keys are stamped. It only updates incoming staging rows, requires a writable ZIP column plus coordinates, skips already-keyed rows when `address_key` exists, and can be disabled with `HLTHPRT_ADDRESS_CANON_ZIP_RESTORE_ENABLED=false` or tuned with `HLTHPRT_ADDRESS_CANON_ZIP_RESTORE_CONCURRENCY` / `HLTHPRT_ADDRESS_CANON_ZIP_RESTORE_SHARDS`.

## Documentation

Public documentation for this repository is organized under [`docs/`](./docs/README.md).

Recommended reading order:

- [Documentation index](./docs/README.md)
- [Architecture overview](./docs/architecture.md)
- [Data sources](./docs/data-sources.md)
- [Import processes](./docs/imports/README.md)

Engineering specs and design notes: see [`specs/`](./specs/). Start with
[`specs/base_arch_prompt.md`](./specs/base_arch_prompt.md) and
[`specs/data_source_registry.md`](./specs/data_source_registry.md).

Per-import documentation:

- [MRF import](./docs/imports/mrf.md)
- [Plan attributes import](./docs/imports/plan-attributes.md)
- [PTG import](./docs/imports/ptg.md)
- [RC/POS code sets import](./docs/imports/code-sets.md)
- [MS-DRG reference import](./docs/imports/ms-drg.md)
- [Clinical reference import](./docs/imports/clinical-reference.md)
- [NPI import](./docs/imports/npi.md)
- [NUCC import](./docs/imports/nucc.md)
- [Geo import](./docs/imports/geo.md)
- [Claims pricing import](./docs/imports/claims-pricing.md)
- [Drug claims import](./docs/imports/drug-claims.md)
- [Provider quality import](./docs/imports/provider-quality.md)
- [PLACES ZCTA import](./docs/imports/places-zcta.md)
- [LODES workplace demand import](./docs/imports/lodes.md)
- [Medicare enrollment import](./docs/imports/medicare-enrollment.md)
- [CMS doctors import](./docs/imports/cms-doctors.md)
- [Facility anchors import](./docs/imports/facility-anchors.md)
- [Pharmacy economics import](./docs/imports/pharmacy-economics.md)
- [Entity address unified import](./docs/imports/entity-address-unified.md)
- [Provider enrichment import](./docs/imports/provider-enrichment.md)
- [Provider Directory FHIR import](./docs/imports/provider-directory-fhir.md)
- [Provider Directory endpoint support matrix](./docs/imports/provider-directory-endpoint-support.md)
- [Part D formulary and pharmacy network import](./docs/imports/partd-formulary-network.md)
- [Pharmacy license import](./docs/imports/pharmacy-license.md)

## Local Development

Use [`.env.example`](./.env.example) as the configuration reference.

Typical local prerequisites:

- Python virtual environment
- PostgreSQL
- Redis

Basic setup:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

Start the API:

```bash
python main.py server start --host 0.0.0.0 --port 8080
```

The API becomes useful after at least one importer has been run successfully.

## API Consumers

The HTTP APIs are intentionally client-neutral. Self-hosted applications,
external gateways, and automation clients can use the same documented public
contracts without depending on a deployment-specific control service.
