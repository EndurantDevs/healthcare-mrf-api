# Physician Pricing + Provider Quality Plan (Current State)

This document captures the active implementation state for physician/provider pricing and quality scoring in `healthcare-mrf-api`.

## Status

Implemented:

1. Claims pricing import (`claims-pricing` / `claims-procedures`) with transactional publish swap.
2. Drug claims import (`drug-claims`) with transactional publish swap.
3. Provider quality import (`provider-quality`) using CMS claims proxies + QPP + SVI.
4. Hard `/score` replacement with Embold-aligned risk-ratio scoring.
5. Cohort-aware benchmarking using geography + specialty + taxonomy + procedure buckets with deterministic fallback.
6. `/pricing/providers/{npi}/score` and `/pricing/physicians/{npi}/score` parity with OpenAPI and tests.

Legacy `quality_proxy_score` narrative is superseded and not the active contract.

## Current Data Model

Pricing core:

1. `pricing_provider`
2. `pricing_procedure`
3. `pricing_provider_procedure`
4. `pricing_provider_procedure_location`
5. `pricing_provider_procedure_cost_profile`
6. `pricing_procedure_peer_stats`
7. `pricing_procedure_geo_benchmark`
8. `pricing_prescription`
9. `pricing_provider_prescription`
10. `code_catalog`
11. `code_crosswalk`

Provider quality:

1. `pricing_qpp_provider`
2. `pricing_svi_zcta`
3. `pricing_provider_quality_feature`
4. `pricing_provider_quality_procedure_lsh`
5. `pricing_provider_quality_peer_target`
6. `pricing_provider_quality_measure`
7. `pricing_provider_quality_domain`
8. `pricing_provider_quality_score`
9. `pricing_quality_run`

## Score Endpoint Contract

Primary endpoint:

- `GET /api/v1/pricing/providers/{npi}/score`

Alias:

- `GET /api/v1/pricing/physicians/{npi}/score`

Query params:

1. `year`
2. `benchmark_mode` (`national|state|zip`)
3. `specialty` (optional cohort override)
4. `taxonomy_code` (optional cohort override)
5. `procedure_codes` (optional CSV cohort override)
6. `procedure_code_system` (`HP_PROCEDURE_CODE|CPT|HCPCS`)
7. `procedure_match_threshold` (`0..1`, default `0.30`)

Response core:

1. `model_version`
2. `tier`
3. `score_0_100`
4. `overall` (`risk_ratio_point`, `ci_75`, `ci_90`)
5. `domains` (`appropriateness`, `effectiveness`, `cost`)
6. `curation_checks`
7. `borderline_status`
8. `cohort_context`
9. `estimated_cost_level` (compatibility helper from cost domain)

## Scoring Mechanics

The active model is risk-ratio based and confidence-aware:

1. Compute observed provider measures.
2. Select peer targets using geography + cohort fallback.
3. Normalize to risk ratio (`RR`) where `RR > 1` means worse than peers.
4. Apply shrinkage and compute confidence intervals (`ci_75`, `ci_90`).
5. Aggregate measure RR into domain RR by geometric mean.
6. Build clinical RR from appropriateness/effectiveness.
7. Build overall RR from clinical/cost (`50% clinical + 50% cost`).
8. Apply tier rules (point estimate + confidence thresholds) and borderline cap.

## Cohort Benchmark Selection

Default behavior is cohort-aware and fallback-based.

Geography fallback:

1. `zip`: `zip -> state -> national`
2. `state`: `state -> national`
3. `national`: `national`

Cohort fallback inside selected geography:

1. `L0`: `specialty + taxonomy + procedure_bucket`
2. `L1`: `specialty + taxonomy`
3. `L2`: `specialty`
4. `L3`: `geo_only`

Procedure similarity uses MinHash/LSH approximation in batch materialization (`target threshold ~0.30`).

## Import and Publish Flow

Provider quality run:

```bash
python main.py start provider-quality
python main.py worker process.ProviderQuality --burst
python main.py worker process.ProviderQuality_finish --burst
```

Test run:

```bash
python main.py start provider-quality --test
python main.py worker process.ProviderQuality --burst
python main.py worker process.ProviderQuality_finish --burst
```

Publish behavior:

1. Stage tables are materialized first.
2. Finalize performs transactional rename swap (`live -> _old`, `stage -> live`).
3. Index archive renames are truncation-safe/idempotent.
4. Finalize acquires a global mutex to avoid cross-pipeline collision.

Operational guardrail:

- Do not run `ClaimsPricing_finish`, `DrugClaims_finish`, and `ProviderQuality_finish` in parallel.

## Key Environment Flags

1. `HLTHPRT_PROVIDER_QUALITY_COHORT_ENABLED=true`
2. `HLTHPRT_PROVIDER_QUALITY_BENCHMARK_MODE=national|state|zip`
3. `HLTHPRT_PROVIDER_QUALITY_ZIP_MIN_PEER_N=30`
4. `HLTHPRT_PROVIDER_QUALITY_MIN_STATE_PEER_N=30`
5. `HLTHPRT_PROVIDER_QUALITY_NATIONAL_MIN_PEER_N=100`
6. `HLTHPRT_PROVIDER_QUALITY_PROCEDURE_MATCH_THRESHOLD=0.30`
7. `HLTHPRT_PROVIDER_QUALITY_MINHASH_NUM_PERM=108`
8. `HLTHPRT_PROVIDER_QUALITY_MINHASH_BANDS=36`
9. `HLTHPRT_PROVIDER_QUALITY_MINHASH_ROWS_PER_BAND=3`

## Validation Baseline

Minimum regression scope:

1. OpenAPI route/param parity.
2. Score API contract tests (default + override path).
3. Cohort fallback SQL/materialization unit tests.
4. Tier threshold edge behavior.
5. Publish swap/index-rename safety checks.

