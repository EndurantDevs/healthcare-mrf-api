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
- [CMS Marketplace Public Use Files](https://www.cms.gov/marketplace/resources/data/public-use-files)
- [CMS State-based Marketplace Public Use Files](https://www.cms.gov/marketplace/resources/data/state-based-public-use-files)
- [NPPES NPI Files](https://download.cms.gov/nppes/NPI_Files.html)
- [CMS Medicare Fee-for-Service Public Provider Enrollment](https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/medicare-fee-for-service-public-provider-enrollment)
- [CMS Quality Payment Program](https://www.cms.gov/Medicare/Quality-Payment-Program/Quality-Payment-Program.html)
- [CDC / ATSDR Social Vulnerability Index](https://www.atsdr.cdc.gov/place-health/php/svi/index.html)
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
| `claims-pricing` | `python main.py start claims-pricing` | CMS Medicare physician claims | provider procedure pricing, peer benchmarks, and procedure geography |
| `claims-procedures` | `python main.py start claims-procedures` | CMS Medicare physician claims | alias of `claims-pricing` |
| `drug-claims` | `python main.py start drug-claims` | CMS Medicare Part D claims | provider-drug and prescription claims aggregates |
| `provider-quality` | `python main.py start provider-quality` | CMS QPP, CDC SVI, Medicare claims | provider quality measures, domain scores, peer targets, and rankings |
| `provider-enrichment` | `python main.py start provider-enrichment` | CMS Medicare FFS public provider enrollment | PECOS-based provider enrollment and relationship enrichment |
| `partd-formulary-network` | `python main.py start partd-formulary-network` | CMS Part D public files | formulary coverage, pharmacy activity, and pharmacy network data |
| `pharmacy-license` | `python main.py start pharmacy-license` | FDA/state board sources | normalized pharmacy license and board-status data |

Detailed run instructions for every importer are documented in [docs/imports/README.md](./docs/imports/README.md).

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
