# Import Processes

This repository has multiple import pipelines. Each command below is a separate operational unit with its own source family and tables.

## Imports at a Glance

| Import | Start command | Finish step | Purpose |
| --- | --- | --- | --- |
| MRF | `python main.py start mrf` | `process.MRF`, `process.MRF_finish`, or `finish mrf` | Marketplace issuer, plan, network, formulary, and transparency ingestion |
| Plan attributes | `python main.py start plan-attributes` | `process.Attributes` worker | Marketplace plan attributes, prices, benefits, and rating areas |
| PTG | `python main.py start ptg` | no separate finish command | Transparency in Coverage table-of-contents and file ingestion |
| NPI | `python main.py start npi` | `process.NPI_finish` worker | NPPES provider directory import |
| NUCC | `python main.py start nucc` | shutdown publish in worker | NUCC taxonomy import |
| Geo | `python main.py start geo` | none | ZIP/city/state lookup support load |
| Claims pricing | `python main.py start claims-pricing` | `process.ClaimsPricing_finish` worker or `finish claims-pricing` | Medicare physician procedure and cost imports |
| Claims procedures | `python main.py start claims-procedures` | same as claims pricing | Alias for claims pricing |
| Drug claims | `python main.py start drug-claims` | `process.DrugClaims_finish` worker or `finish drug-claims` | Medicare Part D provider-drug imports |
| Provider quality | `python main.py start provider-quality` | `process.ProviderQuality_finish` worker or `finish provider-quality` | Quality scoring inputs and provider benchmarks |
| Provider enrichment | `python main.py start provider-enrichment` | `process.ProviderEnrichment` or `process.ProviderEnrichment_finish` | PECOS / Medicare enrollment sidecar import |
| Part D formulary network | `python main.py start partd-formulary-network` | `process.PartDFormularyNetwork_finish` worker or `finish partd-formulary-network` | Medicare Part D pharmacy activity and medication cost data |
| Pharmacy license | `python main.py start pharmacy-license` | `process.PharmacyLicense_finish` worker or `finish pharmacy-license` | State pharmacy board license normalization (direct connectors + machine-readable discovery) |

## Per-import Documentation
- [MRF import](./mrf.md)
- [Plan attributes import](./plan-attributes.md)
- [PTG import](./ptg.md)
- [NPI import](./npi.md)
- [NUCC import](./nucc.md)
- [Geo lookup import](./geo.md)
- [Claims pricing import](./claims-pricing.md)
- [Drug claims import](./drug-claims.md)
- [Provider quality import](./provider-quality.md)
- [Provider enrichment import](./provider-enrichment.md)
- [Part D formulary and pharmacy network import](./partd-formulary-network.md)
- [Pharmacy license import](./pharmacy-license.md)

## Shared Operational Rules
- Use `--test` where supported before large imports.
- Some imports publish by table swap; `_old` tables are expected rollback assets.
- Avoid running `ClaimsPricing_finish` and `DrugClaims_finish` at the same time.
- Run `ProviderQuality_finish` in its own finalize window as well.
