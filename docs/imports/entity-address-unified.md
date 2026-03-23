# Entity Address Unified Import

## Purpose
Builds one unified address/entity search table from already imported internal datasets.
It consolidates provider, facility, and enrollment-side address evidence and applies deterministic inferred-NPI ranking rules.

## Command

```bash
python main.py start entity-address-unified
python main.py worker process.EntityAddressUnified --burst
```

## Source Inputs

This importer does not fetch an external website directly. It materializes from canonical local tables built by other imports, including:

- `npi`
- `provider-enrichment`
- `cms-doctors`
- `facility-anchors`

## Main Table

- `mrf.entity_address_unified`

## Data Notes

- Keeps both direct `npi` and ranked `inferred_npi` outcomes.
- Uses facility-aware ranking logic for ambiguous organization matches (for example hospital/FQHC contexts).
- Supports `primary`, `secondary`, and `practice` address evidence where applicable.
- Canonical dedupe runs inside import using normalized address keys (punctuation/case/zip formatting differences collapse into one row).
- Keeps source evidence on each unified address:
  - `address_sources`
  - `source_record_ids`
  - `source_count`
  - `multi_source_confirmed` (`true` when the same canonical address is observed from 2+ sources)

## Test Mode

```bash
python main.py start entity-address-unified --test
python main.py worker process.EntityAddressUnified --burst
```

## Key Environment Variables

- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_BATCH_SIZE`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SOURCE_CONCURRENCY`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_SHARDS`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_CONCURRENCY`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_ADDITIONAL_INDEXES`
