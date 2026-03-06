# CMS Claims Pricing v1 (HealthPorta-style)

## Scope

This spec documents the first working pricing API for `healthcare-mrf-api` based on CMS claims data imports.

- API surface: HealthPorta-style read-only v1 endpoints only.
- Data source for v1: CMS claims datasets.
- Ribbon API is used only as behavior reference; no Ribbon path or ID parity is implemented.

## Data Sources (v1)

Importer resolves direct CSV URLs from the CMS portal (`https://data.cms.gov/`) and selects the latest CSV distribution for each dataset:

1. Medicare Physician & Other Practitioners - by Provider
2. Medicare Physician & Other Practitioners - by Provider and Service
3. Medicare Physician & Other Practitioners - by Geography and Service

Reporting year window is env-controlled; current operational default is `2023` only:

- `HLTHPRT_CLAIMS_MIN_YEAR=2023`
- `HLTHPRT_CLAIMS_MAX_YEAR=2023`

## Import Process

Command:

```bash
python main.py start claims-pricing
```

Procedures-only alias:

```bash
python main.py start claims-procedures
```

Drug prescriptions import (separate pipeline):

```bash
python main.py start drug-claims
```

Provider quality import (separate pipeline):

```bash
python main.py start provider-quality
python main.py worker process.ProviderQuality --burst
python main.py worker process.ProviderQuality_finish --burst
```

Test mode:

```bash
python main.py start claims-pricing --test
```

Optional import identifier:

```bash
python main.py start claims-pricing --import-id 20260214
```

Pipeline behavior:

1. Resolve latest CSV sources from CMS catalog.
2. Download source CSV files.
3. Stream-split downloaded CSV files into temporary chunks.
4. Enqueue chunk jobs to `arq:ClaimsPricing`.
5. Load chunks into staging tables with dynamic import suffix.
6. Finalize in `arq:ClaimsPricing_finish` and swap staged rows into canonical tables (transactional rename).
7. Keep deterministic sparse sampling in `--test` mode.

Finalize safety notes:

- Publish uses table rename (`live -> _old`, `stage -> live`) and index rename.
- Index archive names are truncation-safe for Postgres 63-char limits and are pre-dropped before rename to avoid `_old` collisions.

### `--test` mode behavior

- Uses deterministic sparse selection (`row_number % 11 == 0`) and row caps.
- Defaults (override via env):
  - `HLTHPRT_CLAIMS_TEST_PROVIDER_ROWS=5000`
  - `HLTHPRT_CLAIMS_TEST_PROVIDER_DRUG_ROWS=10000`
  - `HLTHPRT_CLAIMS_TEST_DRUG_SPENDING_ROWS=2000`
  - `HLTHPRT_CLAIMS_TEST_MAX_DOWNLOAD_BYTES=26214400`
- Chunk and queue defaults (override via env):
  - `HLTHPRT_CLAIMS_CHUNK_TARGET_MB=128`
  - `HLTHPRT_CLAIMS_DOWNLOAD_CONCURRENCY=3`
  - `HLTHPRT_MAX_CLAIMS_JOBS=20`
  - `HLTHPRT_CLAIMS_DEFER_STAGE_INDEXES=true`

## Canonical Tables

Primary procedure/service claims tables:

- `pricing_provider`
- `pricing_procedure`
- `pricing_provider_procedure`
- `pricing_provider_procedure_location`
- `pricing_provider_procedure_cost_profile`
- `pricing_procedure_peer_stats`
- `pricing_procedure_geo_benchmark`

Shared code tables (upserted, not replaced):

- `code_catalog`
- `code_crosswalk`

Prescription tables are owned by `drug-claims`:

- `pricing_prescription`
- `pricing_provider_prescription`

Provider quality tables are owned by `provider-quality`:

- `pricing_qpp_provider`
- `pricing_svi_zcta`
- `pricing_provider_quality_feature`
- `pricing_provider_quality_procedure_lsh`
- `pricing_provider_quality_peer_target`
- `pricing_provider_quality_measure`
- `pricing_provider_quality_domain`
- `pricing_provider_quality_score`
- `pricing_quality_run`

## API Endpoints (v1)

1. `GET /api/v1/codes`
2. `GET /api/v1/codes/{code_system}/{code}`
3. `GET /api/v1/codes/{code_system}/{code}/related`
4. `GET /api/v1/pricing/providers`
5. `GET /api/v1/pricing/providers/{npi}`
6. `GET /api/v1/pricing/providers/{npi}/score`
7. `GET /api/v1/pricing/providers/{npi}/procedures`
8. `GET /api/v1/pricing/providers/{npi}/procedures/{procedure_code}`
9. `GET /api/v1/pricing/providers/{npi}/procedures/{procedure_code}/locations`
10. `GET /api/v1/pricing/procedures/{code_system}/{code}/providers`
11. `GET /api/v1/pricing/procedures/{code_system}/{code}/benchmarks`
12. `GET /api/v1/pricing/providers/{npi}/prescriptions`
13. `GET /api/v1/pricing/providers/{npi}/prescriptions/{rx_code_system}/{rx_code}`
14. `GET /api/v1/pricing/prescriptions/{rx_code_system}/{rx_code}/providers`
15. `GET /api/v1/pricing/prescriptions/{rx_code_system}/{rx_code}/benchmarks`
16. `GET /api/v1/pricing/procedures/autocomplete`
17. `GET /api/v1/pricing/services/autocomplete` (alias)
18. `GET /api/v1/pricing/prescriptions/autocomplete`
19. `GET /api/v1/pricing/drugs/autocomplete` (alias)
20. `GET /api/v1/pricing/providers/by-procedure`
21. `GET /api/v1/pricing/providers/by-service` (alias)
22. `GET /api/v1/pricing/providers/by-prescription`
23. `GET /api/v1/pricing/providers/by-drug` (alias)
24. `GET /api/v1/pricing/physicians/by-service` (alias)
25. `GET /api/v1/pricing/physicians/by-prescription` (alias)
26. `GET /api/v1/pricing/physicians/by-drug` (alias)
27. `GET /api/v1/pricing/providers/{npi}/procedures/{procedure_code}/estimated-cost-level`
28. `GET /api/v1/pricing/providers/{npi}/procedures/{code_system}/{code}/estimated-cost-level`
29. `GET /api/v1/pricing/physicians/{npi}/services/{code_system}/{code}/estimated-cost-level`
30. `GET /api/v1/pricing/procedures/{code_system}/{code}/geo-benchmarks`

Compatibility aliases:

- `GET /api/v1/pricing/physicians` (alias of `/pricing/providers`)
- `GET /api/v1/pricing/physicians/{npi}` (alias of `/pricing/providers/{npi}`)
- `GET /api/v1/pricing/physicians/{npi}/score` (alias of `/pricing/providers/{npi}/score`)
- `GET /api/v1/pricing/physicians/{npi}/services` (alias of `/pricing/providers/{npi}/procedures`)
- `GET /api/v1/pricing/physicians/{npi}/prescriptions` (alias of `/pricing/providers/{npi}/prescriptions`)
- `GET /api/v1/pricing/physicians/{npi}/services/{code_system}/{code}`
- `GET /api/v1/pricing/physicians/{npi}/services/{code_system}/{code}/locations`

## Parameter Coverage

Implemented filters and controls include:

- pagination (`page`, `limit`, `offset` semantics via shared parser)
- sorting (`order`, `order_by`)
- provider filters (`npi`, `state`, `city`, `specialty`, `q`, `year`, `min_claims`, `min_total_cost`)
- procedure filters (`generic_name`, `brand_name`, `q`, `year`, `min_claims`, `min_total_cost`)
- location filters (`state`, `city`, `zip5`, `year`)
- code expansion controls (`code`, `code_system`, `expand_codes`) on pricing procedure lookups

`/pricing/providers/{npi}/score` and alias `/pricing/physicians/{npi}/score` support:

- `year`
- `benchmark_mode` (`national|state|zip`)
- cohort override params:
  - `specialty`
  - `taxonomy_code`
  - `procedure_codes` (CSV)
  - `procedure_code_system` (`HP_PROCEDURE_CODE|CPT|HCPCS`)
  - `procedure_match_threshold` (`0..1`, default `0.30`)

Estimated cost level geography fallback order:

1. `zip5` (if provided)
2. `city + state` (if provided)
3. `state` (if provided)
4. national (`US`)

If a narrower geography has no matching peer group, lookup falls back to broader scope.

Provider quality cohort fallback order for score benchmarking:

1. Geography fallback by mode:
   - `zip`: `zip -> state -> national`
   - `state`: `state -> national`
   - `national`: `national`
2. Cohort fallback inside selected geography:
   - `L0`: `specialty + taxonomy + procedure_bucket`
   - `L1`: `specialty + taxonomy`
   - `L2`: `specialty`
   - `L3`: `geo_only`

## Provider Quality V2 Score Contract

Scoring is Embold-aligned and risk-ratio based:

- measure/domain/overall scores are normalized as risk ratios where `>1` means worse than peer target
- uncertainty is explicitly carried via interval bands (`ci_75`, `ci_90`)
- overall blend is `50% clinical + 50% cost`
- tier checks use fixed threshold logic on point estimate plus confidence checks

Score response includes:

- `model_version`
- `tier`, `borderline_status`, `score_0_100`
- `overall` with `risk_ratio_point`, `ci_75`, `ci_90`
- `domains`: `appropriateness`, `effectiveness`, `cost` (same shape + `evidence_n`)
- `curation_checks`
- `cohort_context` (selected geography/cohort, peer count, whether computed live)
- compatibility helper `estimated_cost_level`

Legacy heuristic-only score fields are not part of the active `/score` contract.

Unsupported in v1:

- write/edit operations
- insurer and hospital transparency datasets
- non-CMS proprietary pricing sources

## Autocomplete + Search Behavior

- Procedures autocomplete deduplicates identical service terms across code systems (`CPT`, `HCPCS`, internal `HP_PROCEDURE_CODE`) and returns merged code variants per term.
- Prescriptions autocomplete matches by generic/brand/rx name and always returns both generic and brand fields in each result (with fallback if one is missing).
- Doctor search endpoints accept geography (`state`, `city`, optional `zip5`) and optional `specialty` for both procedure-driven and prescription-driven discovery.

## Future Extension Points

Planned follow-up inputs:

1. insurer transparency feeds
2. hospital transparency feeds

Recommended extension strategy:

- keep current canonical tables as the read API contract
- add source-specific staging/normalization pipelines
- enrich canonical tables with source provenance and confidence metadata

## Integration Notes

- `claims-pricing` and `drug-claims` may run concurrently for chunk processing.
- Do not run `ClaimsPricing_finish`, `DrugClaims_finish`, and `ProviderQuality_finish` in parallel.
- `npi_address.procedures_array` and `npi_address.medications_array` are populated in NPI import finalization, not in claims/drug finalizers.
