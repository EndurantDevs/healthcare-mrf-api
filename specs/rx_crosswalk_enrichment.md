# HP_RX_CODE External Crosswalk Enrichment

## Goal

Increase external medication code coverage in `mrf.code_crosswalk` so prescription responses can expose `NDC` and `RXNORM` for `HP_RX_CODE` more often.

## Import Behavior

The `drug-claims` finalize path now runs a post-materialization enrichment stage:

1. Baseline identity rows remain (`HP_RX_CODE -> HP_RX_CODE`).
2. Snapshot enrichment joins `mrf.pricing_prescription` against drug-api source tables:
   - `rx_data.product`
   - `rx_data.package`
3. The importer writes bidirectional edges:
   - `HP_RX_CODE -> RXNORM`
   - `HP_RX_CODE -> NDC`
   - reverse edges for both systems.
4. Optional live fallback queries drug-api for unresolved `HP_RX_CODE` rows.

## Configuration

- `HLTHPRT_RX_CROSSWALK_SOURCE=hybrid|snapshot|live` (default: `hybrid`)
- `HLTHPRT_RX_CROSSWALK_LIVE_FALLBACK=true|false` (default: `true`)
- `HLTHPRT_RX_CROSSWALK_CONFIDENCE_MIN=0.85`
- `HLTHPRT_RX_CROSSWALK_MAX_NDC_PER_CODE=5`
- `HLTHPRT_RX_CROSSWALK_MAX_RXNORM_PER_CODE=5`
- `HLTHPRT_RX_CROSSWALK_SNAPSHOT_SCHEMA=rx_data`
- `HLTHPRT_RX_CROSSWALK_SNAPSHOT_PRODUCT_TABLE=product`
- `HLTHPRT_RX_CROSSWALK_SNAPSHOT_PACKAGE_TABLE=package`
- `HLTHPRT_RX_CROSSWALK_LIVE_BASE_URL=http://127.0.0.1:8087`
- `HLTHPRT_RX_CROSSWALK_LIVE_MAX_CODES=400`
- `HLTHPRT_RX_CROSSWALK_LIVE_MAX_PRODUCTS_PER_CODE=12`
- `HLTHPRT_RX_CROSSWALK_LIVE_CONCURRENCY=8`
- `HLTHPRT_RX_CROSSWALK_LIVE_TIMEOUT_SECONDS=12`

## Verification Queries

Coverage of internal codes with at least one external mapping:

```sql
SELECT COUNT(DISTINCT from_code) AS hp_codes_mapped
FROM mrf.code_crosswalk
WHERE UPPER(from_system) = 'HP_RX_CODE'
  AND UPPER(to_system) IN ('NDC', 'RXNORM');
```

Split by external system:

```sql
SELECT UPPER(to_system) AS external_system, COUNT(DISTINCT from_code) AS hp_codes
FROM mrf.code_crosswalk
WHERE UPPER(from_system) = 'HP_RX_CODE'
  AND UPPER(to_system) IN ('NDC', 'RXNORM')
GROUP BY UPPER(to_system)
ORDER BY external_system;
```

Sample edge quality:

```sql
SELECT from_code AS hp_rx_code, to_system, to_code, match_type, confidence, source, updated_at
FROM mrf.code_crosswalk
WHERE UPPER(from_system) = 'HP_RX_CODE'
  AND UPPER(to_system) IN ('NDC', 'RXNORM')
ORDER BY updated_at DESC
LIMIT 50;
```
