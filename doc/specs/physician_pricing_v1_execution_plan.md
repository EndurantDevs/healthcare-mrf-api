# Physician Pricing v1 Execution Plan

This document defines the implementation plan for the physician pricing module in `healthcare-mrf-api`.

## Implementation Status

Completed in current v1 implementation:

1. Env-driven default year resolution (`HLTHPRT_PRICING_DEFAULT_YEAR`).
2. Read-only pricing/provider/service/location endpoints plus procedure-centric lookups.
3. Code catalog and crosswalk tables + API endpoints.
4. `expand_codes` support in pricing lookups.
5. Claims importer population for code catalog and identity crosswalk rows.
6. OpenAPI + unit tests updated for new surface.

## Locked Decisions

1. Data window: `2021, 2022, 2023` only.
2. API default year:
   - Optional query param `year`.
   - If missing, use env `HLTHPRT_PRICING_DEFAULT_YEAR` (default `2023`).
   - No hardcoded default in endpoint logic.
3. Cost band label: `Estimated Cost Level` (`$` to `$$$$$`).
4. Quality metric label: `Quality Proxy Score` (not clinical outcome quality).
5. Code normalization:
   - Add normalized code tables.
   - No foreign keys.
6. Publish strategy:
   - Same as MRF/NPI finalization.
   - Transactional rename swap (`live -> _old`, `staging -> live`) including index rename.
7. Performance target:
   - MRF/NPI-grade ingest speed.
   - Bulk COPY ingest, post-load indexes, no row-by-row fallback in steady state.

## New/Updated Data Model

### A. Code Layer

1. `code_catalog`
- `code_system text`
- `code text`
- `display_name text`
- `short_description text`
- `long_description text`
- `is_active boolean`
- `source text`
- `effective_year int`
- `updated_at timestamp`
- Primary key: `(code_system, code)`

2. `code_crosswalk`
- `from_system text`
- `from_code text`
- `to_system text`
- `to_code text`
- `match_type text` (`exact|narrower|broader|approx`)
- `confidence numeric`
- `source text`
- `updated_at timestamp`
- Primary key: `(from_system, from_code, to_system, to_code)`

3. Optional `code_alias` (only if internal alias/hash mapping is needed for compatibility)
- `alias_system text`
- `alias_code text`
- `code_system text`
- `code text`
- `source text`
- `updated_at timestamp`

### B. Physician Pricing Layer

1. `pricing_physician_provider`
2. `pricing_physician_provider_service`
3. `pricing_physician_geo_service_benchmark`
4. `pricing_physician_service_score`
5. `pricing_physician_score`

## Index Design Principles

1. Order composite indexes by real query patterns first, then selectivity.
2. Avoid single mega-indexes; use separate indexes for distinct access paths.
3. Build heavy secondary indexes after load.

### Planned Indexes (Initial)

1. `code_catalog`
- PK `(code_system, code)`
- `(code_system, display_name)`
- optional trigram on `display_name`

2. `code_crosswalk`
- `(from_system, from_code)`
- `(to_system, to_code)`

3. `pricing_physician_provider`
- `(npi, year)`
- `(state, city, year)`
- `(year, total_claims DESC)` or `(year, total_allowed_amount DESC)` depending on endpoint sorting

4. `pricing_physician_provider_service`
- `(npi, year, code_system, code)`
- `(code_system, code, year, npi)`
- `(year, state, city, code_system, code)`

5. `pricing_physician_score`
- `(npi, year)`
- `(year, quality_proxy_score DESC)`
- `(year, estimated_cost_level, state, city)`

## API Surface (Read-only v1, HealthPorta style)

1. `GET /api/v1/codes`
2. `GET /api/v1/codes/{code_system}/{code}`
3. `GET /api/v1/codes/{code_system}/{code}/related`
4. `GET /api/v1/pricing/physicians`
5. `GET /api/v1/pricing/physicians/{npi}`
6. `GET /api/v1/pricing/physicians/{npi}/services`
7. `GET /api/v1/pricing/physicians/{npi}/services/{code_system}/{code}`
8. `GET /api/v1/pricing/physicians/{npi}/services/{code_system}/{code}/locations`
9. `GET /api/v1/pricing/procedures/{code_system}/{code}/providers`
10. `GET /api/v1/pricing/procedures/{code_system}/{code}/benchmarks`
11. `GET /api/v1/pricing/physicians/{npi}/score`

### Cross-system query support

- Optional `expand_codes=true`.
- If enabled:
  1. Resolve mapped codes via `code_crosswalk`.
  2. Query across all mapped codes.
  3. Return `input_code`, `resolved_codes`, and `matched_via` metadata.

## Scoring Plan

### Estimated Cost Level

- Percentile within peer group (same service + place + geography + year).
- Bands:
  - `0-20`: `$`
  - `20-40`: `$$`
  - `40-60`: `$$$`
  - `60-80`: `$$$$`
  - `80-100`: `$$$$$`

### Quality Proxy Score (0-100)

`0.30*Volume + 0.20*Consistency + 0.20*Efficiency + 0.15*Access + 0.15*DataConfidence`

- Precomputed during import/materialization (not per-request).
- Return with confidence and driver fields.

## Import Pipeline (MRF/NPI-style)

1. Discover source URLs from CMS catalog.
2. Download files with retries/resume/timeouts.
3. Parse CSV asynchronously in large batches.
4. COPY into staging tables with import suffix.
5. Normalize + derive code and pricing entities.
6. Materialize benchmark and score tables in SQL.
7. Build indexes after load.
8. Publish using transactional rename swap.

## Environment Variables (new/updated)

1. `HLTHPRT_PRICING_DEFAULT_YEAR=2023`
2. `HLTHPRT_CLAIMS_IMPORT_BATCH_SIZE=100000`
3. Existing downloader envs stay in use (workers, range size, timeout multipliers, chunk retries).

## 5-Minute Task Breakdown

1. Add `HLTHPRT_PRICING_DEFAULT_YEAR` to `.env.example`.
2. Add `HLTHPRT_CLAIMS_IMPORT_BATCH_SIZE` to `.env.example`.
3. Add config helper to read and validate default year.
4. Add year resolver utility for pricing endpoints.
5. Update provider list endpoint to use resolved year.
6. Update provider procedures endpoint to use resolved year.
7. Update procedure detail endpoint to use resolved year.
8. Update procedure locations endpoint to use resolved year.
9. Include `year_used` in all pricing responses.
10. Add `code_catalog` model.
11. Add `code_crosswalk` model.
12. Add optional `code_alias` model if compatibility needed.
13. Ensure importer bootstraps new code tables (`create_table(..., checkfirst=True)`); migrations are optional.
14. Add physician provider model(s) for new datasets.
15. Add physician provider-service model.
16. Add physician geo-service benchmark model.
17. Add physician service-score model.
18. Add physician overall-score model.
19. Add import command scaffold for physician datasets.
20. Wire command into CLI start group.
21. Add dataset resolution for 3 CMS physician datasets.
22. Add year filtering to 2021-2023.
23. Add downloader step logs and retry instrumentation.
24. Add async parser for provider dataset.
25. Add async parser for provider+service dataset.
26. Add async parser for geo+service dataset.
27. Add bulk COPY stage load for provider rows.
28. Add bulk COPY stage load for provider+service rows.
29. Add bulk COPY stage load for geo+service rows.
30. Materialize `code_catalog` from staged data.
31. Materialize `code_crosswalk` from source mappings.
32. Materialize service benchmark stats.
33. Materialize `Estimated Cost Level` per service.
34. Materialize provider `Quality Proxy Score`.
35. Build post-load indexes for new tables.
36. Add transactional publish function (rename swap + index rename).
37. Replace any copy-based final swap with rename-based publish.
38. Add API endpoint `/codes`.
39. Add API endpoint `/codes/{system}/{code}`.
40. Add API endpoint `/codes/{system}/{code}/related`.
41. Add physician list/detail/service endpoints.
42. Add procedure benchmark/providers endpoints.
43. Add `expand_codes=true` resolution logic.
44. Update OpenAPI with all new schemas/paths.
45. Add tests: importer, contracts, score sanity, code expansion, and publish flow.

## Validation Checklist

1. Import row counts per table logged and reconciled.
2. No duplicate-key fallback spam in steady-state ingest.
3. Publish swap duration measured and logged.
4. API default year behavior verified with and without `year` param.
5. `expand_codes=true` verified against mapped code systems.
6. Score fields and confidence fields present and bounded.
