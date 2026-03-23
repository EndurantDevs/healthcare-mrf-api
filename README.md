# healthcare-mrf-api

[![HealthPorta](https://app.healthporta.com/brand/healthporta-logo-2x.png)](https://app.healthporta.com/docs)

`healthcare-mrf-api` is HealthPorta's data ingestion and API platform for public U.S. healthcare pricing, provider, plan, pharmacy, and Medicare-derived benchmarking data.

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

See the full source catalog in [docs/data-sources.md](./docs/data-sources.md).

## Importers

Each importer is a separate operational pipeline. Together, they build the canonical data used by the API.

| Importer | Start command | Primary source family | Main output |
| --- | --- | --- | --- |
| `mrf` | `python main.py start mrf` | CMS marketplace public use files | issuer, plan, formulary, network, and MRF-linked marketplace data |
| `plan-attributes` | `python main.py start plan-attributes` | CMS marketplace plan attributes files | plan metadata, prices, benefits, and rating-area data |
| `ptg` | `python main.py start ptg` | Transparency in Coverage machine-readable files | TiC table-of-contents and rate-file ingestion |
| `npi` | `python main.py start npi` | NPPES | provider identity, addresses, taxonomies, and directory search data |
| `nucc` | `python main.py start nucc` | NUCC | provider taxonomy lookup tables |
| `geo` | `python main.py start geo` | repository support files | ZIP/city/state/coordinate lookup tables |
| `geo-census` | `python main.py start geo-census` | U.S. Census APIs | ZIP/ZCTA demographic and economic profile metrics |
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

### Direct (non-ARQ) imports

| Import | Command |
| --- | --- |
| `ptg` | `python main.py start ptg [--test]` |
| `geo` | `python main.py start geo [--file /path/to/geo_city_public.csv]` |
| `geo-census` | `python main.py start geo-census [--test]` |

Operational note:
- Do **not** run `ClaimsPricing_finish` and `DrugClaims_finish` at the same time. Both touch shared `code_catalog` and `code_crosswalk`.

## Documentation

Public documentation for this repository is organized under [`docs/`](./docs/README.md).

Recommended reading order:

- [Documentation index](./docs/README.md)
- [Data sources](./docs/data-sources.md)
- [Import processes](./docs/imports/README.md)

Per-import documentation:

- [MRF import](./docs/imports/mrf.md)
- [Plan attributes import](./docs/imports/plan-attributes.md)
- [PTG import](./docs/imports/ptg.md)
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
- [Part D formulary and pharmacy network import](./docs/imports/partd-formulary-network.md)
- [Pharmacy license import](./docs/imports/pharmacy-license.md)

Low-level technical specifications and design notes remain in [`specs/`](./specs/).

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

## Commercial Usage

For managed production access, hosted documentation, and commercial integration support, see [HealthPorta Docs](https://app.healthporta.com/docs).

HealthPorta supports:

- hosted API access for healthcare pricing, provider, plan, and pharmacy data
- integration into internal company systems and client-facing products
- MCP-based connectivity for AI agents and workflow automation

For AI-agent integration details, see [HealthPorta MCP](https://app.healthporta.com/mcp).

## Related Projects

- [HealthPorta](https://www.healthporta.com/about-us)
- [HealthPorta Docs](https://app.healthporta.com/docs)
- [Pharmacy Near Me](https://pharmacy-near-me.com/)
