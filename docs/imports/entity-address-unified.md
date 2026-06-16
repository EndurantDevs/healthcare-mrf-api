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

- Keeps both direct `npi` and deterministic `inferred_npi` outcomes.
- For facility anchors, maps NPI in source order:
  - HRSA `FQHC Site NPI Number` is kept as direct `npi`.
  - CMS PECOS hospital/FQHC enrollment rows are normalized into a CCN->NPI crosswalk and populate `inferred_npi` only when a facility CCN has exactly one distinct NPI.
  - Optional PECOS additional-NPI rows are included in the same uniqueness check when `provider_enrollment_ffs_additional_npi` is available.
  - Additional-NPI matches are also written as review candidates; CCNs with multiple distinct NPIs remain unresolved instead of being auto-selected.
- Uses facility-aware ranking logic for ambiguous organization matches (for example hospital/FQHC contexts).
- Uses canonical `address_key` and primary phone+ZIP matches against NPPES for facility-anchor candidates with hospital/FQHC/clinic taxonomy evidence.
- Adds review-only NPPES DBA candidates for unresolved facility anchors:
  - FQHC DBA name + primary phone + ZIP, gated to FQHC taxonomy.
  - Hospital DBA name + ZIP + state, gated to the expanded NUCC hospital taxonomy set.
  These DBA matches are not auto-inferred; they require review/approval before promotion.
- Writes unresolved facility anchors to `facility_anchor_npi_candidate` with single-candidate, conflict, or no-candidate review status. Rows marked `review_status='approved'` are promoted into `facility_anchor_npi_override` before the next refresh and then participate in inference.
- Keeps a single primary direct/inferred NPI on `entity_address_unified`; additional NPIs remain review candidates/overrides until a separate serving bridge is introduced.
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
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE` (default `false`; retry only, resumes from an already-materialized stage table for the current import id)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_INFERENCE` (default `false`; enables automatic NPI inference updates; review candidates are still populated when this is off)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NAME_FALLBACK_INFERENCE` (default `false`; enables expensive broad name+ZIP+street automatic inference after deterministic facility-anchor matches)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPPES_NAME_INFERENCE` (default `false`; enables expensive NPPES organization-name fallback inference; otherwise those rows flow to the candidate review table)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPPES_BROAD_INFERENCE` (default `false`; enables broad automatic NPPES exact-address and phone+ZIP inference; otherwise those rows remain review candidates)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPI_OTHER_IDENTIFIER_INFERENCE` (default `false`; enables automatic `npi_other_identifier` regex matching for facility anchors)
- `HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_LIMIT` (default `25`)
- `HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_NPPES` (default `false`; broad text-based NPPES review-candidate expansion beyond the default indexed `address_key` and primary phone+ZIP matching)
- `HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_OTHER_IDENTIFIER` (default `false`; includes `npi_other_identifier` regex evidence in the review-candidate table)
