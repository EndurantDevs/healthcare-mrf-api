# Entity Address Unified Import

## Purpose
Builds one unified address/entity search table from already imported internal datasets.
It consolidates provider, facility, and enrollment-side address evidence and applies deterministic inferred-NPI ranking rules.

## Command

```bash
python main.py start entity-address-unified
python main.py worker process.EntityAddressUnified --burst
```

For a PTG-only source refresh after `ptg-address` has published the updated
source projection, use the conservative partial path:

```bash
python main.py start entity-address-unified --refresh-mode ptg-partial --ptg-source-key <source_key>
python main.py worker process.EntityAddressUnified --burst
```

For source snapshot promotions, prefer the chained `ptg-address-entity-refresh`
importer or the import-control `refresh_addresses=true` option documented in
`docs/imports/ptg.md`; it refreshes `ptg_address` first and then runs this
PTG-partial entity refresh against the same source key.

The partial path reuses the published `entity_address_unified` rows for unchanged
locations, reloads only the requested PTG source from `ptg_address`, and still
publishes via the normal stage-table swap. It fails closed and requires
`--refresh-mode full` when the changed PTG source is already merged with base
NPI/MRF evidence or with other PTG sources in live rows.

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
  - Additional-NPI matches are also written as review candidates.
  - When NPPES `npi_taxonomy` is available, CCNs with multiple distinct NPIs are disambiguated by facility taxonomy: among the conflicting NPIs the one whose NPPES taxonomy matches the facility type (`261QF0400X` for FQHC, `HOSPITAL_FACILITY_TAXONOMY_CODES` for hospitals) is selected — preferring the NPI that carries it as the primary taxonomy, otherwise the unique any-taxonomy match. These resolve as `inferred_npi` with method `fqhc_pecos_ccn_taxonomy`/`hospital_pecos_ccn_taxonomy` (confidence `0.95`).
  - CCNs where taxonomy does not isolate a single NPI (multiple NPIs all carry the facility taxonomy, or none does) remain unresolved instead of being auto-selected.
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
python main.py start entity-address-unified --test --limit-per-source 1000
python main.py start entity-address-unified --test --limit-per-source 1000 --publish
python main.py worker process.EntityAddressUnified --burst
```

Test mode is stage-only by default and does not replace the live serving tables
unless `--publish` or import-control `publish=true` is supplied.

## Key Environment Variables

- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_BATCH_SIZE`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SOURCE_CONCURRENCY`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SOURCE_TABLE_SHARDS`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_SHARDS`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_CONCURRENCY`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_SHARDS`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_CONCURRENCY`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_EVIDENCE_SHARDS`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_EVIDENCE_CONCURRENCY`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CONCURRENCY` (default `4`; fan-out for independent support-table inserts after the unified stage is built)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_INDEX_CONCURRENCY` (default `2`; fan-out for support-table index builds)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_ADDITIONAL_INDEXES`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE` (default `false`; retry only, resumes from an already-materialized stage table for the current import id)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_WORK_MEM`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_MAINTENANCE_WORK_MEM`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEMP_FILE_LIMIT` (best effort; skipped automatically when the database role cannot set it)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_LOCK_TIMEOUT`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_STATEMENT_TIMEOUT`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SYNCHRONOUS_COMMIT`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_JIT`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_MAX_PARALLEL_WORKERS_PER_GATHER`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_LIMIT_PER_SOURCE` (bounded pilots only; import-control can also pass `limit_per_source`)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_PUBLISH` (overrides publish decision; by default test mode skips publish and full mode publishes)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SKIP_PUBLISH`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_REFRESH_MODE` (`full` or `ptg-partial`; import-control can also pass `refresh_mode`)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_PTG_SOURCE_KEY` / `HLTHPRT_ENTITY_ADDRESS_UNIFIED_PTG_SOURCE_KEYS` (required for `ptg-partial`; import-control can also pass `ptg_source_key`)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_REQUIRE_ARCHIVE_COORDINATES` (default `false`; when `true`, publish fails if archive rows still lack coordinates)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_INFERENCE` (default `false`; enables automatic NPI inference updates; review candidates are still populated when this is off)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NAME_FALLBACK_INFERENCE` (default `false`; enables expensive broad name+ZIP+street automatic inference after deterministic facility-anchor matches)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPPES_NAME_INFERENCE` (default `false`; enables expensive NPPES organization-name fallback inference; otherwise those rows flow to the candidate review table)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPPES_BROAD_INFERENCE` (default `false`; enables broad automatic NPPES exact-address and phone+ZIP inference; otherwise those rows remain review candidates)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPI_OTHER_IDENTIFIER_INFERENCE` (default `false`; enables automatic `npi_other_identifier` regex matching for facility anchors)
- `HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_LIMIT` (default `25`)
- `HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_NPPES` (default `false`; broad text-based NPPES review-candidate expansion beyond the default indexed `address_key` and primary phone+ZIP matching)
- `HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_OTHER_IDENTIFIER` (default `false`; includes `npi_other_identifier` regex evidence in the review-candidate table)

## Experiment Harness

Entity-address pilots reuse the PTG research harness reporting flow:

```bash
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --suite docs/research/entity_address_unified_benchmark_suite.example.json \
  --dry-run
```

See `docs/research/entity_address_unified_optimization.md` for dev import-control
pilot commands.
